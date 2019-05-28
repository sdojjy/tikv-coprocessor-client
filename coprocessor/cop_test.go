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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/prometheus/common/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetAllTableData(t *testing.T) {
	c := getClient(t)
	defer c.Close()
	//create table t2 (id int , name varchar(25), key `name` (`name`));
	tableInfo, _ := c.GetTableInfo("test", "t2")
	ret, _ := c.ScanTableWithConditionsAndTableInfo(context.Background(), tableInfo)
	printDatum(ret)

	fmt.Println("mock table info scan")
	ret, _ = c.ScanTableWithConditions(context.Background(), InnerTableInfoToMockTableInfo(tableInfo), "id!=1")
	printDatum(ret)
}

func TestSendTableScanRequest(t *testing.T) {
	c := getClient(t)
	defer c.Close()

	tableInfo := &MockTableInfo{
		ID:    1234,
		Names: []string{"id", "score", "name"},
		Types: []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeVarchar)},
	}
	c.AddTableRecord(tableInfo.ID, 1, []types.Datum{types.NewIntDatum(223), types.NewFloat32Datum(3.2), types.NewStringDatum("a")})
	c.AddTableRecord(tableInfo.ID, 2, []types.Datum{types.NewIntDatum(224), types.NewFloat32Datum(3.2), types.NewStringDatum("b")})
	c.AddTableRecord(tableInfo.ID, 3, []types.Datum{types.NewIntDatum(224), types.NewFloat32Datum(3.3), types.NewStringDatum("c")})
	c.AddTableRecord(tableInfo.ID, 4, []types.Datum{types.NewIntDatum(224), types.NewFloat32Datum(3.2), types.NewStringDatum("c")})

	condition := " id in (223, 224) and score < cast(4.3 as unsigned) "
	ret, _ := c.ScanTableWithConditions(context.Background(), tableInfo, condition)
	fmt.Printf("get all records with condition %s\n", condition)
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
	c.SendCoprocessorRequest(context.Background(), tableInfo.ID, returnTypes, executors, rangeFunc, decodeTableRow)
	fmt.Printf("all records with raw cop request\n")
	printDatum(values)
	//assert.Equal(t, 2, len(values))
	//assert.Equal(t, int64(1), values[0][0].GetInt64())
}

func TestSendIndexRequest(t *testing.T) {
	c := getClient(t)
	tableInfo := &MockTableInfo{
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

	c.SendCoprocessorRequest(context.Background(), tableInfo.ID, tableInfo.Types, executors, rangeFunc, decodeTableRow)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, int64(224), values[0][0].GetInt64())
}

func getClient(t *testing.T) *CopClient {
	client, err := NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		t.Fatal("create client failed")
	}
	return client
}
