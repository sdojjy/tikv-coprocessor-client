package coprocessor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/types"
	"strings"
)

type MockTableInfo struct {
	ID    int64
	Names []string
	Types []*types.FieldType
}

func (tableInfo *MockTableInfo) ToInnerTableInfo() *model.TableInfo {
	return &model.TableInfo{
		ID:      tableInfo.ID,
		Name:    model.CIStr{O: "coptestxxxx", L: "coptestxxxx"},
		Columns: tableInfo.GetColumnInfo(),
	}
}

func (tableInfo *MockTableInfo) GetColumnInfo() []*model.ColumnInfo {
	columns := TypesToColumnInfo(tableInfo.Types)
	for index, column := range columns {
		column.Name = model.CIStr{L: strings.ToLower(tableInfo.Names[index]), O: tableInfo.Names[index]}
	}
	return columns
}

func InnerTableInfoToMockTableInfo(tableInfo *model.TableInfo) *MockTableInfo {
	var names []string
	for _, col := range tableInfo.Columns {
		names = append(names, col.Name.O)
	}
	return &MockTableInfo{
		ID:    tableInfo.ID,
		Names: names,
		Types: ColumnInfoToTypes(tableInfo.Columns),
	}
}

func TypesToColumnInfo(types []*types.FieldType) []*model.ColumnInfo {
	var columns []*model.ColumnInfo
	for index, tp := range types {
		column := &model.ColumnInfo{
			ID:     int64(index + 1),
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

func ColumnInfoToTypes(columns []*model.ColumnInfo) []*types.FieldType {
	var typeSlice []*types.FieldType
	for _, col := range columns {
		typeSlice = append(typeSlice, &types.FieldType{Tp: col.Tp, Flen: col.Flen, Decimal: col.Decimal})
	}
	return typeSlice
}

func (c *CopClient) GetTableInfo(dbName, tableName string) (*model.TableInfo, error) {
	schema, err := c.Schema()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tableVal.Meta(), nil
}
