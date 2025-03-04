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

package blockio

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type ReadFilter = func([]*vector.Vector) []int32

func ReadByFilter(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	inputDeletes []int64,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	filter ReadFilter,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (sels []int32, err error) {
	bat, err := LoadColumns(ctx, columns, colTypes, fs, info.MetaLocation(), mp)
	if err != nil {
		return
	}
	var deleteMask nulls.Bitmap

	// merge persisted deletes
	if !info.DeltaLocation().IsEmpty() {
		var persistedDeletes *batch.Batch
		var persistedByCN bool
		// load from storage
		if persistedDeletes, persistedByCN, err = ReadBlockDelete(ctx, info.DeltaLocation(), fs); err != nil {
			return
		}
		var rows *nulls.Nulls
		if persistedByCN {
			rows = evalDeleteRowsByTimestampForDeletesPersistedByCN(persistedDeletes, ts, info.CommitTs)
		} else {
			rows = evalDeleteRowsByTimestamp(persistedDeletes, ts)
		}
		deleteMask.Merge(rows)
	}

	// merge input deletes
	for _, row := range inputDeletes {
		deleteMask.Add(uint64(row))
	}

	sels = filter(bat.Vecs)

	// deslect deleted rows from sels
	if !deleteMask.IsEmpty() {
		var rows []int32
		for _, row := range sels {
			if !deleteMask.Contains(uint64(row)) {
				rows = append(rows, row)
			}
		}
		sels = rows
	}
	return
}

// BlockRead read block data from storage and apply deletes according given timestamp. Caller make sure metaloc is not empty
func BlockRead(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	inputDeletes []int64,
	columns []uint16,
	colTypes []types.Type,
	ts timestamp.Timestamp,
	filterSeqnums []uint16,
	filterColTypes []types.Type,
	filter ReadFilter,
	fs fileservice.FileService,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (*batch.Batch, error) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	var (
		sels []int32
		err  error
	)

	if filter != nil && info.Sorted {
		if sels, err = ReadByFilter(
			ctx, info, inputDeletes, filterSeqnums, filterColTypes,
			types.TimestampToTS(ts), filter, fs, mp,
		); err != nil {
			return nil, err
		}
		if len(sels) == 0 {
			RecordReadFilterSelectivity(1, 1)
		} else {
			RecordReadFilterSelectivity(0, 1)
		}

		if len(sels) == 0 {
			result := batch.NewWithSize(len(colTypes))
			for i, typ := range colTypes {
				if vp == nil {
					result.Vecs[i] = vector.NewVec(typ)
				} else {
					result.Vecs[i] = vp.GetVector(typ)
				}
			}
			return result, nil
		}
	}

	columnBatch, err := BlockReadInner(
		ctx, info, inputDeletes, columns, colTypes,
		types.TimestampToTS(ts), sels, fs, mp, vp,
	)
	if err != nil {
		return nil, err
	}

	columnBatch.SetRowCount(columnBatch.Vecs[0].Length())
	return columnBatch, nil
}

func BlockCompactionRead(
	ctx context.Context,
	location objectio.Location,
	deletes []int64,
	seqnums []uint16,
	colTypes []types.Type,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*batch.Batch, error) {

	loaded, err := LoadColumns(ctx, seqnums, colTypes, fs, location, mp)
	if err != nil {
		return nil, err
	}
	if len(deletes) == 0 {
		return loaded, nil
	}
	result := batch.NewWithSize(len(loaded.Vecs))
	for i, col := range loaded.Vecs {
		typ := *col.GetType()
		result.Vecs[i] = vector.NewVec(typ)
		if err = vector.GetUnionAllFunction(typ, mp)(result.Vecs[i], col); err != nil {
			break
		}
		result.Vecs[i].Shrink(deletes, true)
	}

	if err != nil {
		for _, col := range result.Vecs {
			if col != nil {
				col.Free(mp)
			}
		}
		return nil, err
	}
	result.SetRowCount(result.Vecs[0].Length())
	return result, nil
}

func BlockReadInner(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	inputDeleteRows []int64,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	selectRows []int32, // if selectRows is not empty, it was already filtered by filter
	fs fileservice.FileService,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (result *batch.Batch, err error) {
	var (
		rowidPos    int
		deletedRows []int64
		deleteMask  nulls.Bitmap
		loaded      *batch.Batch
	)

	// read block data from storage specified by meta location
	if loaded, rowidPos, deleteMask, err = readBlockData(
		ctx, columns, colTypes, info, ts, fs, mp, vp,
	); err != nil {
		return
	}

	// assemble result batch for return
	result = batch.NewWithSize(len(loaded.Vecs))

	if len(selectRows) > 0 {
		// NOTE: it always goes here if there is a filter and the block is sorted
		// and there are selected rows after applying the filter and delete mask

		// build rowid column if needed
		if rowidPos >= 0 {
			if loaded.Vecs[rowidPos], err = buildRowidColumn(
				info, selectRows, mp, vp,
			); err != nil {
				return
			}
		}

		// assemble result batch only with selected rows
		for i, col := range loaded.Vecs {
			typ := *col.GetType()
			if typ.Oid == types.T_Rowid {
				result.Vecs[i] = col
				continue
			}
			if vp == nil {
				result.Vecs[i] = vector.NewVec(typ)
			} else {
				result.Vecs[i] = vp.GetVector(typ)
			}
			if err = result.Vecs[i].Union(col, selectRows, mp); err != nil {
				break
			}
		}
		if err != nil {
			for _, col := range result.Vecs {
				if col != nil {
					col.Free(mp)
				}
			}
		}
		return
	}

	// read deletes from storage specified by delta location
	if !info.DeltaLocation().IsEmpty() {
		var deletes *batch.Batch
		var persistedByCN bool
		// load from storage
		if deletes, persistedByCN, err = ReadBlockDelete(ctx, info.DeltaLocation(), fs); err != nil {
			return
		}

		// eval delete rows by timestamp
		var rows *nulls.Nulls
		if persistedByCN {
			rows = evalDeleteRowsByTimestampForDeletesPersistedByCN(deletes, ts, info.CommitTs)
		} else {
			rows = evalDeleteRowsByTimestamp(deletes, ts)
		}

		// merge delete rows
		deleteMask.Merge(rows)

		if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
			logutil.Debugf(
				"blockread %s read delete %d: base %s filter out %v\n",
				info.BlockID.String(), deletes.RowCount(), ts.ToString(), deleteMask.Count())
		}
	}

	// merge deletes from input
	// deletes from storage + deletes from input
	for _, row := range inputDeleteRows {
		deleteMask.Add(uint64(row))
	}

	// Note: it always goes here if no filter or the block is not sorted

	// transform delete mask to deleted rows
	// TODO: avoid this transformation
	if !deleteMask.IsEmpty() {
		deletedRows = deleteMask.ToI64Arrary()
		// logutil.Debugf("deleted/length: %d/%d=%f",
		// 	len(deletedRows),
		// 	loaded.Vecs[0].Length(),
		// 	float64(len(deletedRows))/float64(loaded.Vecs[0].Length()))
	}

	// build rowid column if needed
	if rowidPos >= 0 {
		if loaded.Vecs[rowidPos], err = buildRowidColumn(
			info, nil, mp, vp,
		); err != nil {
			return
		}
	}

	// assemble result batch
	for i, col := range loaded.Vecs {
		typ := *col.GetType()

		if typ.Oid == types.T_Rowid {
			// rowid is already allocted by the mpool, no need to create a new vector
			result.Vecs[i] = col
		} else {
			// for other types, we need to create a new vector
			if vp == nil {
				result.Vecs[i] = vector.NewVec(typ)
			} else {
				result.Vecs[i] = vp.GetVector(typ)
			}
			// copy the data from loaded vector to result vector
			// TODO: avoid this allocation and copy
			if err = vector.GetUnionAllFunction(typ, mp)(result.Vecs[i], col); err != nil {
				break
			}
		}

		// shrink the vector by deleted rows
		if len(deletedRows) > 0 {
			result.Vecs[i].Shrink(deletedRows, true)
		}
	}

	// if any error happens, free the result batch allocated
	if err != nil {
		for _, col := range result.Vecs {
			if col != nil {
				col.Free(mp)
			}
		}
	}
	return
}

func getRowsIdIndex(colIndexes []uint16, colTypes []types.Type) (int, []uint16, []types.Type) {
	idx := -1
	for i, typ := range colTypes {
		if typ.Oid == types.T_Rowid {
			idx = i
			break
		}
	}
	if idx < 0 {
		return idx, colIndexes, colTypes
	}
	idxes := make([]uint16, 0, len(colTypes)-1)
	typs := make([]types.Type, 0, len(colTypes)-1)
	idxes = append(idxes, colIndexes[:idx]...)
	idxes = append(idxes, colIndexes[idx+1:]...)
	typs = append(typs, colTypes[:idx]...)
	typs = append(typs, colTypes[idx+1:]...)
	return idx, idxes, typs
}

func buildRowidColumn(
	info *pkgcatalog.BlockInfo,
	sels []int32,
	m *mpool.MPool,
	vp engine.VectorPool,
) (col *vector.Vector, err error) {
	if vp == nil {
		col = vector.NewVec(objectio.RowidType)
	} else {
		col = vp.GetVector(objectio.RowidType)
	}
	if len(sels) == 0 {
		err = objectio.ConstructRowidColumnTo(
			col,
			&info.BlockID,
			0,
			info.MetaLocation().Rows(),
			m,
		)
	} else {
		err = objectio.ConstructRowidColumnToWithSels(
			col,
			&info.BlockID,
			sels,
			m,
		)
	}
	if err != nil {
		col.Free(m)
		col = nil
	}
	return
}

func readBlockData(
	ctx context.Context,
	colIndexes []uint16,
	colTypes []types.Type,
	info *pkgcatalog.BlockInfo,
	ts types.TS,
	fs fileservice.FileService,
	m *mpool.MPool,
	vp engine.VectorPool,
) (bat *batch.Batch, rowidPos int, deleteMask nulls.Bitmap, err error) {
	rowidPos, idxes, typs := getRowsIdIndex(colIndexes, colTypes)

	readColumns := func(cols []uint16) (result *batch.Batch, loaded *batch.Batch, err error) {
		if len(cols) == 0 && rowidPos >= 0 {
			// only read rowid column on non appendable block, return early
			result = batch.NewWithSize(1)
			// result.Vecs[0] = rowid
			return
		}

		if loaded, err = LoadColumns(ctx, cols, typs, fs, info.MetaLocation(), m); err != nil {
			return
		}

		colPos := 0
		result = batch.NewWithSize(len(colTypes))
		for i, typ := range colTypes {
			if typ.Oid != types.T_Rowid {
				result.Vecs[i] = loaded.Vecs[colPos]
				colPos++
			}
		}
		return
	}

	readABlkColumns := func(cols []uint16) (result *batch.Batch, deletes nulls.Bitmap, err error) {
		var loaded *batch.Batch
		// appendable block should be filtered by committs
		cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if result, loaded, err = readColumns(cols); err != nil {
			return
		}

		t0 := time.Now()
		aborts := vector.MustFixedCol[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedCol[types.TS](loaded.Vecs[len(loaded.Vecs)-2])
		for i := 0; i < len(commits); i++ {
			if aborts[i] || commits[i].Greater(ts) {
				deletes.Add(uint64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), deletes.Count())
		return
	}

	if info.EntryState {
		bat, deleteMask, err = readABlkColumns(idxes)
	} else {
		bat, _, err = readColumns(idxes)
	}

	return
}

func ReadBlockDelete(ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService) (bat *batch.Batch, isPersistedByCN bool, err error) {
	isPersistedByCN, err = persistedByCN(ctx, deltaloc, fs)
	if err != nil {
		return
	}
	if isPersistedByCN {
		bat, err = LoadColumns(ctx, []uint16{0, 1}, nil, fs, deltaloc, nil)
		if err != nil {
			return
		}
		return
	} else {
		bat, err = LoadColumns(ctx, []uint16{0, 1, 2, 3}, nil, fs, deltaloc, nil)
		if err != nil {
			return
		}
		return
	}
}

func persistedByCN(ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService) (bool, error) {
	meta, err := objectio.FastLoadObjectMeta(ctx, &deltaloc, fs)
	if err != nil {
		return false, err
	}
	blkmeta := meta.GetBlockMeta(uint32(deltaloc.ID()))
	columnCount := blkmeta.GetColumnCount()
	return columnCount == 2, nil
}

func evalDeleteRowsByTimestamp(deletes *batch.Batch, ts types.TS) (rows *nulls.Bitmap) {
	if deletes == nil {
		return
	}
	// record visible delete rows
	rows = nulls.NewWithSize(0)
	rowids := vector.MustFixedCol[types.Rowid](deletes.Vecs[0])
	tss := vector.MustFixedCol[types.TS](deletes.Vecs[1])
	aborts := vector.MustFixedCol[bool](deletes.Vecs[3])

	for i, rowid := range rowids {
		if aborts[i] || tss[i].Greater(ts) {
			continue
		}
		row := rowid.GetRowOffset()
		rows.Add(uint64(row))
	}
	return
}

func evalDeleteRowsByTimestampForDeletesPersistedByCN(deletes *batch.Batch, ts types.TS, committs types.TS) (rows *nulls.Bitmap) {
	if deletes == nil || ts.Less(committs) {
		return
	}
	// record visible delete rows
	rows = nulls.NewWithSize(0)
	rowids := vector.MustFixedCol[types.Rowid](deletes.Vecs[0])

	for _, rowid := range rowids {
		row := rowid.GetRowOffset()
		rows.Add(uint64(row))
	}
	return
}

// BlockPrefetch is the interface for cn to call read ahead
// columns  Which columns should be taken for columns
// service  fileservice
// infos [s3object name][block]
func BlockPrefetch(idxes []uint16, service fileservice.FileService, infos [][]*pkgcatalog.BlockInfo) error {
	// Generate prefetch task
	for i := range infos {
		// build reader
		pref, err := BuildPrefetchParams(service, infos[i][0].MetaLocation())
		if err != nil {
			return err
		}
		for _, info := range infos[i] {
			pref.AddBlock(idxes, []uint16{info.MetaLocation().ID()})
			if !info.DeltaLocation().IsEmpty() {
				// Need to read all delete
				err = Prefetch([]uint16{0, 1, 2}, []uint16{info.DeltaLocation().ID()}, service, info.DeltaLocation())
				if err != nil {
					return err
				}
			}
		}
		err = pipeline.Prefetch(pref)
		if err != nil {
			return err
		}
	}
	return nil
}

func RecordReadFilterSelectivity(hit, total int) {
	pipeline.stats.RecordReadFilterSelectivity(hit, total)
}

func RecordBlockSelectivity(hit, total int) {
	pipeline.stats.RecordBlockSelectivity(hit, total)
}

func RecordColumnSelectivity(hit, total int) {
	pipeline.stats.RecordColumnSelectivity(hit, total)
}

func ExportSelectivityString() string {
	return pipeline.stats.ExportString()
}
