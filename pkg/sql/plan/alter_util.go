package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"strings"
)

func isDroppableColumn(tblInfo *TableDef, colName string, ctx CompilerContext) error {
	if len(tblInfo.Cols) == 1 {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop only column %s in table %s", colName, tblInfo.Name)
	}

	// We do not support drop column that contain primary key columns now.
	err := checkDropColumnWithPrimaryKey(colName, tblInfo.Pkey, ctx)
	if err != nil {
		return err
	}
	// We do not support drop column that contain index columns now.
	err = checkDropColumnWithIndex(colName, tblInfo.Indexes, ctx)
	if err != nil {
		return err
	}
	// We do not support drop column that contain foreign key columns now.
	err = checkDropColumnWithForeignKey(colName, tblInfo.Fkeys, ctx)
	if err != nil {
		return err
	}

	// We do not support drop column for partitioned table now
	err = checkDropColumnWithPartitionKeys(colName, tblInfo, ctx)
	if err != nil {
		return err
	}

	return nil
}

func checkDropColumnWithPrimaryKey(colName string, pkey *plan.PrimaryKeyDef, ctx CompilerContext) error {
	for _, column := range pkey.Names {
		if column == colName {
			return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column %s with Primary Key covered now", colName)
		}
	}
	return nil
}

func checkDropColumnWithIndex(colName string, indexes []*plan.IndexDef, ctx CompilerContext) error {
	for _, indexInfo := range indexes {
		if indexInfo.Unique {
			for _, column := range indexInfo.Parts {
				if column == colName {
					return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column %s with unique index covered now", colName)
				}
			}
		}
	}
	return nil
}

func checkDropColumnWithForeignKey(colName string, fkeys []*ForeignKeyDef, ctx CompilerContext) error {
	if len(fkeys) > 0 {
		// We do not support drop column that dependent foreign keys constraints
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column for partition table now")
	}
	return nil
}

func checkDropColumnWithPartitionKeys(colName string, tblInfo *TableDef, ctx CompilerContext) error {
	if tblInfo.Partition != nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "can't drop column for partition table now")
	}
	return nil
}

func checkIsDroppableColumn(tableDef *TableDef, colName string, ctx CompilerContext) error {
	// Check whether dropped column has existed.
	col := FindColumn(tableDef.Cols, colName)
	if col == nil {
		//err = dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
		return moerr.NewInvalidInput(ctx.GetContext(), "Can't DROP '%-.192s'; check that column/key exists", colName)
	}

	if err := isDroppableColumn(tableDef, colName, ctx); err != nil {
		return err
	}
	return nil
}

// FindColumn finds column in cols by name.
func FindColumn(cols []*ColDef, name string) *ColDef {
	for _, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return col
		}
	}
	return nil
}
