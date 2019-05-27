package coprocessor

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/types"
	"strings"
)

type TableInfo struct {
	ID    int64
	Names []string
	Types []*types.FieldType
}

func (tableInfo *TableInfo) ToInnerTableInfo() *model.TableInfo {
	return &model.TableInfo{
		ID:      tableInfo.ID,
		Name:    model.CIStr{O: "coptestxxxx", L: "coptestxxxx"},
		Columns: tableInfo.GetColumnInfo(),
	}
}

func (tableInfo *TableInfo) GetColumnInfo() []*model.ColumnInfo {
	columns := TypesToColumnInfo(tableInfo.Types)
	for index, column := range columns {
		column.Name = model.CIStr{L: strings.ToLower(tableInfo.Names[index]), O: tableInfo.Names[index]}
	}
	return columns
}

func TypesToColumnInfo(types []*types.FieldType) []*model.ColumnInfo {
	var columns []*model.ColumnInfo
	for index, tp := range types {
		column := &model.ColumnInfo{
			ID:     int64(index),
			Offset: index,
			State:  model.StatePublic,
		}
		column.Tp = tp.Tp
		column.Flen = tp.Flen
		column.Decimal = tp.Decimal
		columns = append(columns, column)
	}
	return columns
}
