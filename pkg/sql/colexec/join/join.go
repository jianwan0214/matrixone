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

package join

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" inner join ")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	for i := range ap.ctr.evecs {
		ap.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[0][i])
		if err != nil {
			return err
		}
	}

	if ap.Cond != nil {
		ap.ctr.expr, err = colexec.NewExpressionExecutor(proc, ap.Cond)
	}
	return err
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(proc, anal); err != nil {
				return process.ExecNext, err
			}
			if ctr.mp == nil {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return process.ExecNext, err
			}

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.RowCount() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if ctr.bat == nil || ctr.bat.RowCount() == 0 {
				proc.PutBatch(bat)
				continue
			}
			if err := ctr.probe(bat, ap, proc, anal, isFirst, isLast); err != nil {
				return process.ExecNext, err
			}
			return process.ExecNext, nil

		default:
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}
	}
}

func (ctr *container) build(proc *process.Process, anal process.Analyze) error {
	bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
	if err != nil {
		return err
	}

	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.DupJmAuxData()
		anal.Alloc(ctr.mp.Map().Size())
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	defer proc.PutBatch(bat)
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = proc.GetVector(*bat.Vecs[rp.Pos].GetType())
			// for inner join, if left batch is sorted , then output batch is sorted
			rbat.Vecs[i].SetSorted(bat.Vecs[rp.Pos].GetSorted())
		} else {
			rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		rbat.Clean(proc.Mp())
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.bat, proc.Mp())
	}

	mSels := ctr.mp.Sels()

	count := bat.RowCount()
	itr := ctr.mp.Map().NewIterator()
	rowCount := 0
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)

		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			sels := mSels[vals[k]-1]
			if ap.Cond != nil {
				for _, sel := range sels {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.bat, int64(sel),
						1, ctr.cfs2); err != nil {
						return err
					}
					vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2})
					if err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					}
					bs := vector.MustFixedCol[bool](vec)
					if !bs[0] {
						continue
					}
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						} else {
							if err := rbat.Vecs[j].UnionOne(ctr.bat.Vecs[rp.Pos], int64(sel), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
					rowCount++
				}
			} else {
				for j, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := rbat.Vecs[j].UnionMulti(bat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
							rbat.Clean(proc.Mp())
							return err
						}
					} else {
						if err := rbat.Vecs[j].Union(ctr.bat.Vecs[rp.Pos], sels, proc.Mp()); err != nil {
							rbat.Clean(proc.Mp())
							return err
						}
					}
				}
				rowCount += len(sels)
			}
		}
	}

	rbat.AddRowCount(rowCount)
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			ctr.cleanEvalVectors()
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
