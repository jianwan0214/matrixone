// Copyright 2021 Matrix Origin
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

package engine

import (
	"time"

	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/computation"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
)

var _ engine.Engine = &TpeEngine{}
var _ engine.Database = &TpeDatabase{}
var _ engine.Relation = &TpeRelation{}
var _ engine.Reader = &TpeReader{}

type TpeConfig struct {
	KvType     tuplecodec.KVType
	SerialType tuplecodec.SerializerType

	//cubeKV needs CubeDriver
	Cube driver.CubeDriver

	//the count of rows per write or scan
	KVLimit uint64

	ParallelReader bool

	TpeDedupSetBatchTimeout  time.Duration
	TpeDedupSetBatchTrycount int
	PBKV                     *pebble.Storage
}

type TpeEngine struct {
	tpeConfig      *TpeConfig
	dh             descriptor.DescriptorHandler
	computeHandler computation.ComputationHandler
}

type TpeDatabase struct {
	id             uint64
	desc           *descriptor.DatabaseDesc
	computeHandler computation.ComputationHandler
}

type TpeRelation struct {
	id             uint64
	dbDesc         *descriptor.DatabaseDesc
	desc           *descriptor.RelationDesc
	computeHandler computation.ComputationHandler
	nodes          engine.Nodes
	shards         *tuplecodec.Shards
}

type ShardNode struct {
	//the address of the store of the leader replica of the shard
	Addr string
	//the id of the store of the leader replica of the shard
	ID uint64
	//the bytes of the id
	IDbytes string
}

type ShardInfo struct {
	//the startKey and endKey of the Shard
	startKey []byte
	endKey   []byte
	//the key for the next scan
	nextScanKey []byte
	//scan shard completely?
	completeInShard bool
	node            ShardNode
}

type TpeReader struct {
	dbDesc         *descriptor.DatabaseDesc
	tableDesc      *descriptor.RelationDesc
	computeHandler computation.ComputationHandler
	readCtx        *tuplecodec.ReadContext
	shardInfos     []ShardInfo
	parallelReader bool
	//for test
	isDumpReader bool
	id           int
	dumpData	 bool
	opt 		 *batch.DumpOption
}

func GetTpeReaderInfo(r *TpeRelation, eng *TpeEngine, opt *batch.DumpOption) *TpeReader {
	return &TpeReader{
		dbDesc:	r.dbDesc,
		tableDesc: r.desc,
		computeHandler: eng.computeHandler,
		opt: opt,
		dumpData: true,
	}
}

func MakeReadParam(r *TpeRelation) (refCnts []uint64, attrs []string) {
    refCnts = make([]uint64, len(r.desc.Attributes))
	attrs = make([]string, len(refCnts))
	for i, attr := range r.desc.Attributes {
		attrs[i] = attr.Name
	}
	return refCnts, attrs
} 