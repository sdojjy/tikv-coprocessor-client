package coprocessor

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/prometheus/common/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetRegions(t *testing.T) {
	c := getClient(t)
	tableInfo, _ := c.GetTableInfo("test", "a")

	// for record
	startKey, _ := tablecodec.GetTableHandleKeyRange(tableInfo.ID)
	region, perr, _ := c.PdClient.GetRegion(context.Background(), startKey)
	fmt.Printf("%v, %v,", region, perr)
	tableRegion, err := c.GetTableRegion(tableInfo.ID)
	fmt.Printf("%v, %v", tableRegion, err)
}

func TestGetTableInfo(t *testing.T) {
	dbInfo, err := getClient(t).GetTableInfo("mysql", "tidb")
	fmt.Printf("%v, %v", dbInfo, err)
}

func Test_range(t *testing.T) {
	full := ranger.FullRange()
	dbInfo, _ := getClient(t).GetTableInfo("mysql", "user")
	kv := distsql.TableRangesToKVRanges(dbInfo.ID, full, nil)
	fmt.Printf("%v", kv)
}

func TestAddTableRecord(t *testing.T) {
	c := getClient(t)
	rawData := []types.Datum{types.NewIntDatum(223), types.NewStringDatum("xxxabcdefg")}
	//tableInfo := &TableInfo{ID:123}
	c.AddTableRecord(123, 1, rawData)
}

func TestAddIndexRecord(t *testing.T) {
	c := getClient(t)
	rawData := []types.Datum{types.NewIntDatum(223), types.NewStringDatum("xxxabcdefg")}
	//tableInfo := &TableInfo{ID:123}
	c.AddIndexRecord(123, 1, 1, rawData, false)
}

func TestSendTableScanRequest(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	tableInfo := &TableInfo{
		ID:    1234,
		Names: []string{"id", "score", "name"},
		Types: []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeVarchar)},
	}
	c.AddTableRecord(tableInfo.ID, 1, []types.Datum{types.NewIntDatum(223), types.NewFloat32Datum(3.2), types.NewStringDatum("a")})
	c.AddTableRecord(tableInfo.ID, 2, []types.Datum{types.NewIntDatum(224), types.NewFloat32Datum(3.2), types.NewStringDatum("b")})
	c.AddTableRecord(tableInfo.ID, 3, []types.Datum{types.NewIntDatum(224), types.NewFloat32Datum(3.3), types.NewStringDatum("c")})
	c.AddTableRecord(tableInfo.ID, 4, []types.Datum{types.NewIntDatum(224), types.NewFloat32Datum(3.2), types.NewStringDatum("c")})

	ret, _ := c.ScanTableWithConditions(context.Background(), tableInfo, " id in (223, 224) and score < 3.3 ")
	printDatum(ret)

	var values [][]types.Datum
	decodeTableRow := func(row chunk.Row, fs []*types.FieldType) error {
		var rowValue []types.Datum
		for idx, f := range fs {
			rowValue = append(rowValue, row.GetDatum(idx, f))
		}
		values = append(values, rowValue)
		return nil
	}

	expr, err := c.ParseExpress(tableInfo, " id>223 ")
	if err != nil {
		log.Fatal(err)
	}

	col := &expression.Column{
		RetType: types.NewFieldType(mysql.TypeFloat),
		ID:      1,
		Index:   1,
	}

	groupById := &expression.Column{
		RetType: types.NewFieldType(mysql.TypeVarchar),
		ID:      2,
		Index:   2,
	}

	agg, err := c.GenAggExprPB(ast.AggFuncMax, []expression.Expression{col}, false)
	//agg, err := c.GenAggExprPB(ast.AggFuncAvg, []expression.Expression{col}, false)
	//agg, err := c.GenAggExprPB(ast.AggFuncSum, []expression.Expression{col}, false)
	//agg, err := c.GenAggExprPB(ast.AggFuncMin, []expression.Expression{col}, false)
	//agg, err := c.GenAggExprPB(ast.AggFuncFirstRow, []expression.Expression{col}, false)
	if err != nil {
		log.Fatal(err)
	}

	returnTypes := []*types.FieldType{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeVarchar)}
	executors := []*tipb.Executor{
		NewTableScanExecutorWithTypes(tableInfo.ID, tableInfo.Types, false),
		NewSelectionScanExecutor([]*tipb.Expr{expr}),
		NewAggregationExecutor([]*tipb.Expr{agg}, []*tipb.Expr{c.GetGroupByPB(groupById)}),
		//NewTopNExecutor(1, []*tipb.ByItem{}),
		NewLimitExecutor(2),
	}

	rangeFunc := func() *copRanges {
		full := ranger.FullIntRange(false)
		keyRange := distsql.TableRangesToKVRanges(tableInfo.ID, full, nil)
		return &copRanges{mid: keyRange}
	}
	c.SendCoprocessorRequest(context.Background(), tableInfo, returnTypes, executors, rangeFunc, decodeTableRow)
	printDatum(values)
	//assert.Equal(t, 2, len(values))
	//assert.Equal(t, int64(1), values[0][0].GetInt64())
}

func TestSendIndexRequest(t *testing.T) {
	c := getClient(t)
	tableInfo := &TableInfo{
		ID:    1234,
		Names: []string{"id", "name"},
		Types: []*types.FieldType{types.NewFieldType(mysql.TypeLong), types.NewFieldType(mysql.TypeVarchar)},
	}
	indexId := int64(12)
	c.AddIndexRecord(tableInfo.ID, indexId, 1, []types.Datum{types.NewIntDatum(222), types.NewStringDatum("a")}, false)
	c.AddIndexRecord(tableInfo.ID, indexId, 2, []types.Datum{types.NewIntDatum(223), types.NewStringDatum("b")}, false)
	c.AddIndexRecord(tableInfo.ID, indexId, 3, []types.Datum{types.NewIntDatum(224), types.NewStringDatum("c")}, false)
	c.AddIndexRecord(tableInfo.ID, indexId, 4, []types.Datum{types.NewIntDatum(224), types.NewStringDatum("c")}, false)

	var values [][]types.Datum
	decodeTableRow := func(row chunk.Row, fs []*types.FieldType) error {
		var rowValue []types.Datum
		for idx, f := range fs {
			rowValue = append(rowValue, row.GetDatum(idx, f))
		}
		values = append(values, rowValue)
		return nil
	}

	expr, err := c.ParseExpress(tableInfo, " id=224 ")
	if err != nil {
		log.Fatal(err)
	}

	executors := []*tipb.Executor{
		NewIndexScanExecutor(tableInfo, indexId, false),
		NewSelectionScanExecutor([]*tipb.Expr{expr}),
		NewTopNExecutor(13, nil),
		NewLimitExecutor(1),
	}

	rangeFunc := func() *copRanges {
		full := ranger.FullIntRange(false)
		keyRange, _ := distsql.IndexRangesToKVRanges(&stmtctx.StatementContext{InSelectStmt: true}, tableInfo.ID, indexId, full, nil)
		return &copRanges{mid: keyRange}
	}

	c.SendCoprocessorRequest(context.Background(), tableInfo, tableInfo.Types, executors, rangeFunc, decodeTableRow)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, int64(224), values[0][0].GetInt64())
}

func getClient(t *testing.T) *ClusterClient {
	client, err := NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		t.Fatal("create client failed")
	}
	return client
}

func printDatum(values [][]types.Datum) {
	for _, row := range values {
		for _, col := range row {
			str, _ := col.ToString()
			fmt.Printf("%s\t", str)
		}
		fmt.Println()
	}
}
