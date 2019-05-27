package coprocessor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/prometheus/common/log"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *ClusterClient) AddTableRecord(tableId int64, rowId int64, rowData []types.Datum) error {
	timezone := time.UTC
	val, err := EncodeRowValue(rowData, timezone)
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

func (c *ClusterClient) AddIndexRecord(tableId, indexId int64, rowId int64,
	indexColumnData []types.Datum, unique bool) error {
	timezone := time.UTC
	val := []byte(fmt.Sprintf("%d", rowId))
	key, err := GenIndexKey(&stmtctx.StatementContext{TimeZone: timezone}, tableId, indexId, indexColumnData, rowId, unique)
	if err != nil {
		return err
	}
	return c.Put(key, val)
}

func (c *ClusterClient) SendCoprocessorRequest(ctx context.Context,
	tableInfo *TableInfo,
	returnTypes []*types.FieldType,
	executors []*tipb.Executor,
	getCopRange func() *copRanges,
	decodeTableRow func(chunk.Row, []*types.FieldType) error) error {
	regionIds, err := c.GetRecordRegionIds(tableInfo.ID)
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
			log.Fatal(err)
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

func (c *ClusterClient) SendIndexScanRequest(ctx context.Context,
	tableInfo *TableInfo,
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
	err := c.SendCoprocessorRequest(ctx, tableInfo, returnTypes, executors, rangeFunc, decodeTableRow)
	return values, err
}

func (c *ClusterClient) ScanIndexWithConditions(ctx context.Context, tableInfo *TableInfo, indexId int64, conditions ...string) ([][]types.Datum, error) {
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
	err := c.SendCoprocessorRequest(ctx, tableInfo, tableInfo.Types, executors, rangeFunc, decodeTableRow)
	return values, err
}

func (c *ClusterClient) ScanTableWithConditions(ctx context.Context, tableInfo *TableInfo, conditions ...string) ([][]types.Datum, error) {
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
		NewTableScanExecutor(tableInfo, false),
		NewSelectionScanExecutor(expList),
	}

	rangeFunc := func() *copRanges {
		full := ranger.FullIntRange(false)
		keyRange := distsql.TableRangesToKVRanges(tableInfo.ID, full, nil)
		return &copRanges{mid: keyRange}
	}
	err := c.SendCoprocessorRequest(ctx, tableInfo, tableInfo.Types, executors, rangeFunc, decodeTableRow)
	return values, err
}

type selectResult struct {
	respChkIdx int
	fieldTypes []*types.FieldType

	selectResp *tipb.SelectResponse
	location   *time.Location
}

func parseResponse(resp *coprocessor.Response, fs []*types.FieldType, decodeTableRow func(chunk.Row, []*types.FieldType) error) {
	var data []byte = resp.Data
	selectResp := new(tipb.SelectResponse)
	err := selectResp.Unmarshal(data)
	if err != nil {
		log.Fatal("parse response failed", err)
	}
	if selectResp.Error != nil {
		log.Fatal("query failed ", selectResp.Error)
	}
	location, _ := time.LoadLocation("")
	chk := chunk.New(fs, 1024, 4096)
	r := selectResult{selectResp: selectResp, fieldTypes: fs, location: location}
	for r.respChkIdx < len(r.selectResp.Chunks) && len(r.selectResp.Chunks[r.respChkIdx].RowsData) != 0 {
		r.readRowsData(chk)
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err = decodeTableRow(row, r.fieldTypes)
			if err != nil {
				log.Fatal("decode failed", err)
			}
		}
		chk.Reset()
		r.respChkIdx++
	}
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	decoder := codec.NewDecoder(chk, r.location)
	for !chk.IsFull() && len(rowsData) > 0 {
		for i := 0; i < len(r.fieldTypes); i++ {
			rowsData, err = decoder.DecodeOne(rowsData, i, r.fieldTypes[i])
			if err != nil {
				return err
			}
		}
	}
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData
	return nil
}
