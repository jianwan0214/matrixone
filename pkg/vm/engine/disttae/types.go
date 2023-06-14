// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	PREFETCH_THRESHOLD = 512
	PREFETCH_ROUNDS    = 32
)

const (
	INSERT = iota
	DELETE
	COMPACTION_CN
	UPDATE
	ALTER
)

const (
	MO_DATABASE_ID_NAME_IDX       = 1
	MO_DATABASE_ID_ACCOUNT_IDX    = 2
	MO_DATABASE_LIST_ACCOUNT_IDX  = 1
	MO_TABLE_ID_NAME_IDX          = 1
	MO_TABLE_ID_DATABASE_ID_IDX   = 2
	MO_TABLE_ID_ACCOUNT_IDX       = 3
	MO_TABLE_LIST_DATABASE_ID_IDX = 1
	MO_TABLE_LIST_ACCOUNT_IDX     = 2
	MO_PRIMARY_OFF                = 2
	INIT_ROWID_OFFSET             = math.MaxUint32
)

var (
	_ client.Workspace = (*Transaction)(nil)
)

var GcCycle = 10 * time.Second

type DNStore = metadata.DNService

type IDGenerator interface {
	AllocateID(ctx context.Context) (uint64, error)
	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
}

type Engine struct {
	Flag bool
	sync.RWMutex
	mp         *mpool.MPool
	fs         fileservice.FileService
	ls         lockservice.LockService
	cli        client.TxnClient
	idGen      IDGenerator
	catalog    *cache.CatalogCache
	dnID       string
	partitions map[[2]uint64]*logtailreplay.Partition
	packerPool *fileservice.Pool[*types.Packer]

	// XXX related to cn push model
	pClient pushClient
}

// Transaction represents a transaction
type Transaction struct {
	sync.Mutex
	engine *Engine
	// readOnly default value is true, once a write happen, then set to false
	readOnly atomic.Bool
	// db       *DB
	// blockId starts at 0 and keeps incrementing,
	// this is used to name the file on s3 and then give it to tae to use
	// not-used now
	// blockId uint64

	// local timestamp for workspace operations
	meta *txn.TxnMeta
	op   client.TxnOperator

	// writes cache stores any writes done by txn
	writes []Entry
	// txn workspace size
	workspaceSize uint64

	dnStores []DNStore
	proc     *process.Process

	idGen IDGenerator

	// interim incremental rowid
	rowId [6]uint32
	segId types.Uuid
	// use to cache table
	tableMap *sync.Map
	// use to cache database
	databaseMap *sync.Map
	// use to cache created table
	createMap *sync.Map
	/*
		for deleted table
		CORNER CASE
		create table t1(a int);
		begin;
		drop table t1; // t1 does not exist
		select * from t1; // can not access t1
		create table t2(a int); // new table
		drop table t2;
		create table t2(a int,b int); //new table
		commit;
		show tables; //no table t1; has table t2(a,b)
	*/
	deletedTableMap *sync.Map

	//deletes info for blocks generated by CN through writing S3.
	deletedBlocks *deletedBlocks

	// block id on S3 -> position in txn.writes
	cnBlkId_Pos                     map[types.Blockid]Pos
	blockId_raw_batch               map[types.Blockid]*batch.Batch
	blockId_dn_delete_metaLoc_batch map[types.Blockid][]*batch.Batch

	batchSelectList map[*batch.Batch][]int64

	rollbackCount int
	statementID   int
	statements    []int
}

type Pos struct {
	idx    int
	offset int64
}

// FIXME: The map inside this one will be accessed concurrently, using
// a mutex, not sure if there will be performance issues
type deletedBlocks struct {
	sync.RWMutex

	// used to store cn block's deleted rows
	// blockId => deletedOffsets
	offsets map[types.Blockid][]int64
}

func (b *deletedBlocks) addDeletedBlocks(blockID *types.Blockid, offsets []int64) {
	b.Lock()
	defer b.Unlock()
	b.offsets[*blockID] = append(b.offsets[*blockID], offsets...)
}

func (b *deletedBlocks) getDeletedOffsetsByBlock(blockID *types.Blockid) []int64 {
	b.RLock()
	defer b.RUnlock()
	res := b.offsets[*blockID]
	offsets := make([]int64, len(res))
	copy(offsets, res)
	return offsets
}

func (b *deletedBlocks) removeBlockDeletedInfos(ids []*types.Blockid) {
	b.Lock()
	defer b.Unlock()
	for _, id := range ids {
		delete(b.offsets, *id)
	}
}

func (b *deletedBlocks) iter(fn func(*types.Blockid, []int64) bool) {
	b.RLock()
	defer b.RUnlock()
	for id, offsets := range b.offsets {
		if !fn(&id, offsets) {
			return
		}
	}
}

func (txn *Transaction) PutCnBlockDeletes(blockId *types.Blockid, offsets []int64) {
	txn.deletedBlocks.addDeletedBlocks(blockId, offsets)
}

func (txn *Transaction) IncrStatemenetID(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()
	if txn.statementID > 0 {
		start := txn.statements[txn.statementID-1]
		writes := make([]Entry, 0, len(txn.writes[start:]))
		for i := start; i < len(txn.writes); i++ {
			if txn.writes[i].typ == DELETE {
				writes = append(writes, txn.writes[i])
			}
		}
		for i := start; i < len(txn.writes); i++ {
			if txn.writes[i].typ != DELETE {
				writes = append(writes, txn.writes[i])
			}
		}
		txn.writes = append(txn.writes[:start], writes...)
	}
	txn.statements = append(txn.statements, len(txn.writes))
	txn.statementID++
	if txn.meta.IsRCIsolation() &&
		(txn.statementID > 1 || txn.rollbackCount > 0) {
		if err := txn.op.UpdateSnapshot(
			ctx,
			timestamp.Timestamp{}); err != nil {
			return err
		}
		txn.resetSnapshot()
	}
	return nil
}

func (txn *Transaction) RollbackLastStatement(ctx context.Context) error {
	txn.Lock()
	defer txn.Unlock()

	txn.rollbackCount++
	if txn.statementID > 0 {
		txn.statementID--
		end := txn.statements[txn.statementID]
		for i := end; i < len(txn.writes); i++ {
			if txn.writes[i].bat == nil {
				continue
			}
			txn.writes[i].bat.Clean(txn.engine.mp)
		}
		txn.writes = txn.writes[:end]
		txn.statements = txn.statements[:txn.statementID]
	}
	return nil
}
func (txn *Transaction) resetSnapshot() error {
	txn.tableMap.Range(func(key, value interface{}) bool {
		value.(*txnTable).resetSnapshot()
		return true
	})
	return nil
}

// Entry represents a delete/insert
type Entry struct {
	typ          int
	tableId      uint64
	databaseId   uint64
	tableName    string
	databaseName string
	// blockName for s3 file
	fileName string
	// update or delete tuples
	bat       *batch.Batch
	dnStore   DNStore
	pkChkByDN int8
	/*
		if truncate is true,it denotes the Entry with typ DELETE
		on mo_tables is generated by the Truncate operation.
	*/
	truncate bool
}

// isGeneratedByTruncate denotes the entry is yielded by the truncate operation.
func (e *Entry) isGeneratedByTruncate() bool {
	return e.typ == DELETE &&
		e.databaseId == catalog.MO_CATALOG_ID &&
		e.tableId == catalog.MO_TABLES_ID &&
		e.truncate
}

// txnDatabase represents an opened database in a transaction
type txnDatabase struct {
	databaseId        uint64
	databaseName      string
	databaseType      string
	databaseCreateSql string
	txn               *Transaction
}

type tableKey struct {
	accountId  uint32
	databaseId uint64
	tableId    uint64
	name       string
}

type databaseKey struct {
	accountId uint32
	id        uint64
	name      string
}

// txnTable represents an opened table in a transaction
type txnTable struct {
	sync.Mutex

	tableId   uint64
	version   uint32
	tableName string
	dnList    []int
	db        *txnDatabase
	//	insertExpr *plan.Expr
	defs       []engine.TableDef
	tableDef   *plan.TableDef
	seqnums    []uint16
	typs       []types.Type
	_partState *logtailreplay.PartitionState
	dirtyBlks  []catalog.BlockInfo

	// blockInfos stores all the block infos for this table of this transaction
	// it is only generated when the table is not created by this transaction
	// it is initialized by updateBlockInfos and once it is initialized, it will not be updated
	blockInfos []catalog.BlockInfo

	// specify whether the blockInfos is updated. once it is updated, it will not be updated again
	blockInfosUpdated bool
	// specify whether the logtail is updated. once it is updated, it will not be updated again
	logtailUpdated bool

	primaryIdx    int // -1 means no primary key
	primarySeqnum int // -1 means no primary key
	clusterByIdx  int // -1 means no clusterBy key
	viewdef       string
	comment       string
	partitioned   int8   //1 : the table has partitions ; 0 : no partition
	partition     string // the info about partitions when the table has partitions
	relKind       string
	createSql     string
	constraint    []byte

	//entries belong to this table,and come from txn.writes.
	writes []Entry
	// offset of the writes in workspace
	writesOffset int

	// localState stores uncommitted data
	localState *logtailreplay.PartitionState
	// this should be the statement id
	// but seems that we're not maintaining it at the moment
	// localTS timestamp.Timestamp
	//rowid in mo_tables
	rowid types.Rowid
	//rowids in mo_columns
	rowids []types.Rowid
	//the old table id before truncate
	oldTableId uint64
}

type column struct {
	accountId  uint32
	tableId    uint64
	databaseId uint64
	// column name
	name            string
	tableName       string
	databaseName    string
	typ             []byte
	typLen          int32
	num             int32
	comment         string
	notNull         int8
	hasDef          int8
	defaultExpr     []byte
	constraintType  string
	isClusterBy     int8
	isHidden        int8
	isAutoIncrement int8
	hasUpdate       int8
	updateExpr      []byte
	seqnum          uint16
}

type withFilterMixin struct {
	ctx      context.Context
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	tableDef *plan.TableDef

	// columns used for reading
	columns struct {
		seqnums  []uint16
		colTypes []types.Type
		// colNulls []bool

		compPKPositions []int // composite primary key pos in the columns

		pkPos    int // -1 means no primary key in columns
		rowidPos int // -1 means no rowid in columns

		indexOfFirstSortedColumn int
	}

	filterState struct {
		evaluated bool
		expr      *plan.Expr
		filter    blockio.ReadFilter
	}
}

type blockReader struct {
	withFilterMixin

	// used for prefetch
	infos       [][]*catalog.BlockInfo
	steps       []int
	currentStep int

	// block list to scan
	blks []*catalog.BlockInfo
}

type blockMergeReader struct {
	withFilterMixin

	table  *txnTable
	blks   []catalog.BlockInfo
	buffer []int64
}

type mergeReader struct {
	rds []engine.Reader
}

type emptyReader struct {
}

//type ModifyBlockMeta struct {
//	meta    catalog.BlockInfo
//	deletes []int64
//}

type pkRange struct {
	isRange bool
	items   []int64
	ranges  []int64
}
