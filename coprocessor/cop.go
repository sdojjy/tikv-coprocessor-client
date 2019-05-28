package coprocessor

import (
	"context"
	"errors"
	"fmt"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"math"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
)

//send the coprocessor request to tikv
//
//tableId:   the id of the table
//returnTypes: column types that should be return by the coprocessor, set correct value base on the executors
//executors:  the executors that send to tikv coprocessor
//getCopRange: a func that return range of the coprocessor request
//  example:
//  getCopRange := func() *copRanges {
//		full := ranger.FullIntRange(false)
//		keyRange, _ := distsql.IndexRangesToKVRanges(&stmtctx.StatementContext{InSelectStmt: true}, tableInfo.ID, indexId, full, nil)
//		return &copRanges{mid: keyRange}
//	}
//decodeTableRow: a func that decode the row record
//  example:
//  	var values [][]types.Datum
//	decodeTableRow := func(row chunk.Row, fs []*types.FieldType) error {
//		var rowValue []types.Datum
//		for idx, f := range fs {
//			rowValue = append(rowValue, row.GetDatum(idx, f))
//		}
//		values = append(values, rowValue)
//		return nil
//	}
//
func (c *CopClient) SendCoprocessorRequest(ctx context.Context,
	tableId int64,
	returnTypes []*types.FieldType,
	executors []*tipb.Executor,
	getCopRange func() *copRanges,
	decodeTableRow func(chunk.Row, []*types.FieldType) error) error {
	regionIds, err := c.GetRecordRegionIds(tableId)
	if err != nil {
		return err
	}

	var offsets []uint32
	for i := 0; i < len(returnTypes); i++ {
		offsets = append(offsets, uint32(i))
	}
	dag := &tipb.DAGRequest{
		StartTs:       math.MaxInt64,
		OutputOffsets: offsets,
		Executors:     executors,
	}

	copRange := getCopRange()
	data, _ := dag.Marshal()
	request := &tikvrpc.Request{
		Type: tikvrpc.CmdCop,
		Cop: &coprocessor.Request{
			Tp:     kv.ReqTypeDAG,
			Data:   data,
			Ranges: copRange.toPBRanges(),
		},

		Context: kvrpcpb.Context{
			HandleTime: true,
			ScanDetail: true,
		},
	}

	for _, regionId := range regionIds {
		bo := tikv.NewBackoffer(context.Background(), 20000)
		keyLocation, _ := c.RegionCache.LocateRegionByID(bo, regionId)
		rpcContext, _ := c.RegionCache.GetRPCContext(bo, keyLocation.Region)
		if e := tikvrpc.SetContext(request, rpcContext.Meta, rpcContext.Peer); e != nil {
			return e
		}

		addr, err := c.loadStoreAddr(context.Background(), bo, rpcContext.Peer.StoreId)
		if err != nil {
			return err
		}

		tikvResp, err := c.RpcClient.SendRequest(ctx, addr, request, readTimeout)
		if err != nil {
			return err
		}
		resp := tikvResp.Cop
		if resp.RegionError != nil || resp.OtherError != "" {
			return errors.New(fmt.Sprintf("coprocessor grpc call failed %s,%s", resp.RegionError, resp.OtherError))
		}

		if resp.Data != nil {
			parseResponse(resp, returnTypes, decodeTableRow)
		}
	}
	return nil
}

func (c *CopClient) SendIndexScanRequest(ctx context.Context,
	tableInfo *MockTableInfo,
	indexId int64,
	returnTypes []*types.FieldType,
	executors []*tipb.Executor) ([][]types.Datum, error) {

	var values [][]types.Datum
	decodeTableRow := func(row chunk.Row, fs []*types.FieldType) error {
		var rowValue []types.Datum
		for idx, f := range fs {
			rowValue = append(rowValue, row.GetDatum(idx, f))
		}
		values = append(values, rowValue)
		return nil
	}

	rangeFunc := func() *copRanges {
		full := ranger.FullIntRange(false)
		keyRange, _ := distsql.IndexRangesToKVRanges(&stmtctx.StatementContext{InSelectStmt: true}, tableInfo.ID, indexId, full, nil)
		return &copRanges{mid: keyRange}
	}
	err := c.SendCoprocessorRequest(ctx, tableInfo.ID, returnTypes, executors, rangeFunc, decodeTableRow)
	return values, err
}

func (c *CopClient) ScanIndexWithConditions(ctx context.Context, tableInfo *MockTableInfo, indexId int64, conditions ...string) ([][]types.Datum, error) {
	var values [][]types.Datum
	decodeTableRow := func(row chunk.Row, fs []*types.FieldType) error {
		var rowValue []types.Datum
		for idx, f := range fs {
			rowValue = append(rowValue, row.GetDatum(idx, f))
		}
		values = append(values, rowValue)
		return nil
	}

	var expList []*tipb.Expr
	for _, exprStr := range conditions {
		expr, err := c.ParseExpress(tableInfo, exprStr)
		if err != nil {
			return nil, err
		}
		expList = append(expList, expr)
	}

	executors := []*tipb.Executor{
		NewIndexScanExecutor(tableInfo, indexId, false),
		NewSelectionScanExecutor(expList),
	}

	rangeFunc := func() *copRanges {
		full := ranger.FullIntRange(false)
		keyRange, _ := distsql.IndexRangesToKVRanges(&stmtctx.StatementContext{InSelectStmt: true}, tableInfo.ID, indexId, full, nil)
		return &copRanges{mid: keyRange}
	}
	err := c.SendCoprocessorRequest(ctx, tableInfo.ID, tableInfo.Types, executors, rangeFunc, decodeTableRow)
	return values, err
}

func (c *CopClient) ScanTableWithExpressionsAndTableInfo(ctx context.Context, tableInfo *model.TableInfo, expList []*tipb.Expr) ([][]types.Datum, error) {
	var values [][]types.Datum
	decodeTableRow := func(row chunk.Row, fs []*types.FieldType) error {
		var rowValue []types.Datum
		for idx, f := range fs {
			rowValue = append(rowValue, row.GetDatum(idx, f))
		}
		values = append(values, rowValue)
		return nil
	}

	executors := []*tipb.Executor{
		NewTableScanExecutorWithTableInfo(tableInfo, false),
		NewSelectionScanExecutor(expList),
	}

	rangeFunc := func() *copRanges {
		full := ranger.FullIntRange(false)
		keyRange := distsql.TableRangesToKVRanges(tableInfo.ID, full, nil)
		return &copRanges{mid: keyRange}
	}
	err := c.SendCoprocessorRequest(ctx, tableInfo.ID, ColumnInfoToTypes(tableInfo.Columns), executors, rangeFunc, decodeTableRow)
	return values, err
}

func (c *CopClient) ScanTableWithConditionsAndTableInfo(ctx context.Context, tableInfo *model.TableInfo, conditions ...string) ([][]types.Datum, error) {
	var expList []*tipb.Expr
	for _, exprStr := range conditions {
		expr, err := c.ParseExprWithTableInfo(tableInfo, exprStr)
		if err != nil {
			return nil, err
		}
		expList = append(expList, expr)
	}

	return c.ScanTableWithExpressionsAndTableInfo(ctx, tableInfo, expList)
}

func (c *CopClient) ScanTableWithConditions(ctx context.Context, tableInfo *MockTableInfo, conditions ...string) ([][]types.Datum, error) {
	return c.ScanTableWithConditionsAndTableInfo(ctx, tableInfo.ToInnerTableInfo(), conditions...)
}
