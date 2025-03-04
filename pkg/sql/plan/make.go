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

package plan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func MakePlan2Decimal64ExprWithType(v types.Decimal64, typ *Type) *plan.Expr {
	rawA := int64(v)
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: false,
				Value: &plan.Const_Decimal64Val{
					Decimal64Val: &plan.Decimal64{
						A: rawA,
					},
				},
			},
		},
	}
}

func MakePlan2Decimal128ExprWithType(v types.Decimal128, typ *Type) *plan.Expr {
	rawA := v.B0_63
	rawB := v.B64_127
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: false,
				Value: &plan.Const_Decimal128Val{
					Decimal128Val: &plan.Decimal128{
						A: int64(rawA),
						B: int64(rawB),
					},
				},
			},
		},
	}
}

func makePlan2DecimalExprWithType(ctx context.Context, v string, isBin ...bool) (*plan.Expr, error) {
	_, scale, err := types.Parse128(v)
	if err != nil {
		return nil, err
	}
	var typ *plan.Type
	if scale < 18 && len(v) < 18 {
		typ = &plan.Type{
			Id:          int32(types.T_decimal64),
			Width:       18,
			Scale:       scale,
			NotNullable: true,
		}
	} else {
		typ = &plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       38,
			Scale:       scale,
			NotNullable: true,
		}
	}
	return appendCastBeforeExpr(ctx, makePlan2StringConstExprWithType(v, isBin...), typ)
}

func makePlan2DateConstNullExpr(t types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(t),
			NotNullable: false,
		},
	}
}

func makePlan2Decimal128ConstNullExpr() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       38,
			Scale:       0,
			NotNullable: false,
		},
	}
}

func makePlan2NullConstExprWithType() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	}
}

func makePlan2BoolConstExpr(v bool) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Bval{
			Bval: v,
		},
	}}
}

func makePlan2BoolConstExprWithType(v bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2BoolConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
	}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_I64Val{
			I64Val: v,
		},
	}}
}

var MakePlan2Int64ConstExprWithType = makePlan2Int64ConstExprWithType

func makePlan2Int64ConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}
}

func makePlan2Uint64ConstExpr(v uint64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_U64Val{
			U64Val: v,
		},
	}}
}

func makePlan2Uint64ConstExprWithType(v uint64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Uint64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_uint64),
			NotNullable: true,
		},
	}
}

func makePlan2Float64ConstExpr(v float64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Dval{
			Dval: v,
		},
	}}
}

var MakePlan2Float64ConstExprWithType = makePlan2Float64ConstExprWithType

func makePlan2Float64ConstExprWithType(v float64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Float64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: true,
		},
	}
}

func makePlan2StringConstExpr(v string, isBin ...bool) *plan.Expr_C {
	c := &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Sval{
			Sval: v,
		},
	}}
	if len(isBin) > 0 {
		c.C.IsBin = isBin[0]
	}
	return c
}

var MakePlan2StringConstExprWithType = makePlan2StringConstExprWithType

func makePlan2StringConstExprWithType(v string, isBin ...bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2StringConstExpr(v, isBin...),
		Typ: &plan.Type{
			Id:          int32(types.T_varchar),
			NotNullable: true,
			Width:       int32(utf8.RuneCountInString(v)),
		},
	}
}

func makePlan2NullTextConstExpr(v string) *plan.Expr_C {
	c := &plan.Expr_C{C: &plan.Const{
		Isnull: true,
	}}
	return c
}

func MakePlan2NullTextConstExprWithType(v string) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2NullTextConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_text),
			NotNullable: false,
			Width:       int32(utf8.RuneCountInString(v)),
		},
	}
}

func makePlan2CastExpr(ctx context.Context, expr *Expr, targetType *Type) (*Expr, error) {
	if isSameColumnType(expr.Typ, targetType) {
		return expr, nil
	}
	targetType.NotNullable = expr.Typ.NotNullable
	t1, t2 := makeTypeByPlan2Expr(expr), makeTypeByPlan2Type(targetType)
	if types.T(expr.Typ.Id) == types.T_any {
		expr.Typ = targetType
		return expr, nil
	}
	fGet, err := function.GetFunctionByName(ctx, "cast", []types.Type{t1, t2})
	if err != nil {
		return nil, err
	}
	t := &plan.Expr{
		Typ: targetType,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{
				Typ: targetType,
			},
		},
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{Obj: fGet.GetEncodedOverloadID(), ObjName: "cast"},
				Args: []*Expr{expr, t},
			},
		},
		Typ: targetType,
	}, nil
}

// if typ is decimal128 and decimal64 without scalar and width
// set a default value for it.
func rewriteDecimalTypeIfNecessary(typ *plan.Type) *plan.Type {
	if typ.Id == int32(types.T_decimal128) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 10
		typ.Width = 38 // width
	}
	if typ.Id == int32(types.T_decimal64) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 2
		typ.Width = 6 // width
	}
	return typ
}

var MakePlan2Type = makePlan2Type

func makePlan2Type(typ *types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

var MakeTypeByPlan2Type = makeTypeByPlan2Type

func makeTypeByPlan2Type(typ *plan.Type) types.Type {
	oid := types.T(typ.Id)
	return types.New(oid, typ.Width, typ.Scale)
}

var MakeTypeByPlan2Expr = makeTypeByPlan2Expr

func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	oid := types.T(expr.Typ.Id)
	return types.New(oid, expr.Typ.Width, expr.Typ.Scale)
}

func makeHiddenColTyp() *Type {
	return &Type{
		Id:    int32(types.T_varchar),
		Width: types.MaxVarcharLen,
	}
}

// used for Compound primary key column name && clusterby column name
func MakeHiddenColDefByName(name string) *ColDef {
	return &ColDef{
		Name:   name,
		Hidden: true,
		Typ:    makeHiddenColTyp(),
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
}

func MakeRowIdColDef() *ColDef {
	return &ColDef{
		Name:   catalog.Row_ID,
		Hidden: true,
		Typ: &Type{
			Id: int32(types.T_Rowid),
		},
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
}

func isSameColumnType(t1 *Type, t2 *Type) bool {
	if t1.Id != t2.Id {
		return false
	}
	if t1.Width == t2.Width && t1.Scale == t2.Scale {
		return true
	}
	return true
}

// GetColDefFromTable Find the target column definition from the predefined
// table columns and return its deep copy
func GetColDefFromTable(Cols []*ColDef, hidenColName string) *ColDef {
	for _, coldef := range Cols {
		if coldef.Name == hidenColName {
			return DeepCopyColDef(coldef)
		}
	}
	panic("Unable to find target column from predefined table columns")
}
