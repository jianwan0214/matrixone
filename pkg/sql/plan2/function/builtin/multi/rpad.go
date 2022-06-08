// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/rpad"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Rpad(origVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if origVecs[0].IsScalarNull() || origVecs[1].IsScalarNull() || origVecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(origVecs[0].Typ), nil
	}

	for _, vec := range origVecs {
		fmt.Println("wangjian sql0a is", vec.Col, vec.Nsp)
	}
	
	fmt.Println("wangjian sql0b is")
	isConst := []bool{origVecs[0].IsScalar(), origVecs[1].IsScalar(), origVecs[2].IsScalar()}

	// gets all args
	strs, sizes, padstrs := origVecs[0].Col.(*types.Bytes), origVecs[1].Col, origVecs[2].Col
	oriNsps := []*nulls.Nulls{origVecs[0].Nsp, origVecs[1].Nsp, origVecs[2].Nsp}

	fmt.Println("wangjian sql0c is")
	if origVecs[0].IsScalar() {
		fmt.Println("wangjian sql0d is")
		return nil, errors.New("the first argument of the lpad function can not be a constant")
	}

	// gets a new vector to store our result
	resultVec, err := proc.AllocVector(origVecs[0].Typ, 24*int64(len(strs.Lengths)))
	if err != nil {
		fmt.Println("wangjian sql0e is")
		return nil, err
	}
	result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
	if err != nil {
		fmt.Println("wangjian sql0f is")
		return nil, err
	}
	resultVec.Nsp = nsp
	vector.SetCol(resultVec, result)
	
	fmt.Println("wangjian sql0z is", resultVec)
	return resultVec, nil
}
