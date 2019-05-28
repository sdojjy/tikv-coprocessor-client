package coprocessor

import (
	"fmt"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"time"
)

// insert a table record to tikv
func (c *CopClient) AddTableRecordWithTimezone(tableId int64, rowId int64, rowData []types.Datum, colIndex []int, timezone *time.Location) error {
	val, err := EncodeRowValue(rowData, colIndex, timezone)
	if err != nil {
		return err
	}

	key := GenRecordKey(tableId, rowId)
	err = c.Put(key, val)
	if err != nil {
		return err
	}
	return nil
}

func (c *CopClient) AddTableRecordWithColIndex(tableId int64, rowId int64, rowData []types.Datum, colIndex []int) error {
	return c.AddTableRecordWithTimezone(tableId, rowId, rowData, colIndex, time.UTC)
}

func (c *CopClient) AddTableRecord(tableId int64, rowId int64, rowData []types.Datum) error {
	var colIdx []int
	//column index start from 1
	for index, _ := range rowData {
		colIdx = append(colIdx, index+1)
	}
	return c.AddTableRecordWithColIndex(tableId, rowId, rowData, colIdx)
}

//insert a index record to tikv
func (c *CopClient) AddIndexRecordWithTimezone(tableId, indexId int64, rowId int64,
	indexColumnData []types.Datum, unique bool, timezone *time.Location) error {
	val := []byte(fmt.Sprintf("%d", rowId))
	key, err := GenIndexKey(&stmtctx.StatementContext{TimeZone: timezone}, tableId, indexId, indexColumnData, rowId, unique)
	if err != nil {
		return err
	}
	return c.Put(key, val)
}

func (c *CopClient) AddIndexRecord(tableId, indexId int64, rowId int64,
	indexColumnData []types.Datum, unique bool) error {
	return c.AddIndexRecordWithTimezone(tableId, indexId, rowId, indexColumnData, unique, time.UTC)
}
