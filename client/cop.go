package client

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/prometheus/common/log"
	"math"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
)

func SendCopIndexScanRequest(ctx context.Context, tableInfo *model.TableInfo, c *ClusterClient) error {
	regionInfa, _ := c.GetTableRegion(tableInfo.ID)

	for _, region := range regionInfa.RecordRegions {

		_, peer, _ := c.PdClient.GetRegionByID(ctx, region.ID)
		log.Info("region id: ", region.ID)

		bo := tikv.NewBackoffer(context.Background(), 20000)
		keyLocation, _ := c.RegionCache.LocateRegionByID(bo, region.ID)

		rpcContext, _ := c.RegionCache.GetRPCContext(bo, keyLocation.Region)

		var idxColx []*model.ColumnInfo
		idxColx = append(idxColx, model.FindColumnInfo(tableInfo.Columns, "int_idx"))
		idxColx = append(idxColx, tableInfo.Columns[0])

		dag := &tipb.DAGRequest{
			StartTs:       math.MaxInt64,
			OutputOffsets: []uint32{0, 1},
			Executors: []*tipb.Executor{
				{
					Tp: tipb.ExecType_TypeIndexScan,
					IdxScan: &tipb.IndexScan{
						TableId: tableInfo.ID,
						IndexId: regionInfa.Indices[0].ID,
						Columns: model.ColumnsToProto(idxColx, tableInfo.PKIsHandle),
						Desc:    false},
				},
			},
		}

		full := ranger.FullIntRange(false)
		keyRange, _ := distsql.IndexRangesToKVRanges(&stmtctx.StatementContext{InSelectStmt: true}, tableInfo.ID, tableInfo.Indices[0].ID, full, nil)
		copRange := &copRanges{mid: keyRange}
		data, _ := dag.Marshal()

		request := &tikvrpc.Request{
			Type: tikvrpc.CmdCop,
			Cop: &coprocessor.Request{
				Tp:     kv.ReqTypeDAG,
				Data:   data,
				Ranges: copRange.toPBRanges(),
			},

			Context: kvrpcpb.Context{
				//IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
				//Priority:       kvPriorityToCommandPri(worker.req.Priority),
				//NotFillCache:   worker.req.NotFillCache,
				HandleTime: true,
				ScanDetail: true,
			},
		}

		if e := tikvrpc.SetContext(request, rpcContext.Meta, rpcContext.Peer); e != nil {
			log.Fatal(e)
		}

		addr, err := c.loadStoreAddr(ctx, bo, peer.StoreId)
		if err != nil {
			log.Fatal(err)
		}
		tikvResp, err := c.RpcClient.SendRequest(ctx, addr, request, readTimeout)
		if err != nil {
			return err
		}
		resp := tikvResp.Cop
		if resp.RegionError != nil || resp.OtherError != "" {
			log.Fatal("coprocessor grpc call failed", resp.RegionError, resp.OtherError)
		}

		if resp.Data != nil {
			parseResponse(resp, []*types.FieldType{types.NewFieldType(mysql.TypeLong), types.NewFieldType(mysql.TypeLong)})
		}

	}
	return nil
}

func SendCopTableScanRequest(ctx context.Context, tableInfo *model.TableInfo, c *ClusterClient) error {
	regionInfa, _ := c.GetTableRegion(tableInfo.ID)
	for _, region := range regionInfa.RecordRegions {
		_, peer, _ := c.PdClient.GetRegionByID(ctx, region.ID)
		log.Info("region id: ", region.ID)

		bo := tikv.NewBackoffer(context.Background(), 20000)
		keyLocation, _ := c.RegionCache.LocateRegionByID(bo, region.ID)

		rpcContext, _ := c.RegionCache.GetRPCContext(bo, keyLocation.Region)

		collectSummary := true
		dag := &tipb.DAGRequest{
			StartTs:       math.MaxInt64,
			OutputOffsets: []uint32{0, 1, 2},
			//Flags:226,
			//TimeZoneName: "Asia/Chongqing",
			//TimeZoneOffset:28800,
			//EncodeType:tipb.EncodeType_TypeDefault,
			CollectExecutionSummaries: &collectSummary,
		}

		columnInfo := model.ColumnsToProto(tableInfo.Columns, tableInfo.PKIsHandle)
		executor1 := tipb.Executor{
			Tp: tipb.ExecType_TypeTableScan,
			TblScan: &tipb.TableScan{
				TableId: tableInfo.ID,
				Columns: columnInfo,
				Desc:    false,
			},
		}

		ss, _ := session.CreateSession(c.Storage)
		expr, _ := expression.ParseSimpleExprWithTableInfo(ss, "VARIABLE_NAME !='' ", tableInfo)
		exprPb := expression.NewPBConverter(c.Storage.GetClient(), &stmtctx.StatementContext{InSelectStmt: true}).ExprToPB(expr)
		executor2 := tipb.Executor{
			Tp: tipb.ExecType_TypeSelection,
			Selection: &tipb.Selection{
				Conditions: []*tipb.Expr{exprPb},
			},
		}

		//stmts, _, err :=parser.New().Parse("select count(id) from test.a", "", "")
		//parser.New().Parse(exprStr, "", "")
		//for _, warn := range warns {
		//	ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
		//}
		//if err != nil {
		//
		//}

		//groupBy := []expression.Expression{childCols[1]}
		//col := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
		//aggFunc := aggregation.NewAggFuncDesc(ss, "count", []expression.Expression{col}, false)
		//aggFuncs := []*aggregation.AggFuncDesc{aggFunc}
		//aggExprPb := aggregation.AggFuncToPBExpr(&stmtctx.StatementContext{InSelectStmt: true}, c.Storage.GetClient(), aggFunc)
		//pbConvert := expression.NewPBConverter(c.Storage.GetClient(), &stmtctx.StatementContext{InSelectStmt: true})
		//groupByExprPb := pbConvert.ExprToPB(col)
		//groupByExprPb := expression.GroupByItemToPB(&stmtctx.StatementContext{InSelectStmt: true}, c.Storage.GetClient(), col)

		//aggExpr, _ := expression.ParseSimpleExprWithTableInfo(ss, " count(id) ", tableInfo)
		//aggExprPb := expression.NewPBConverter(c.Storage.GetClient(),&stmtctx.StatementContext{InSelectStmt: true}).ExprToPB(aggExpr)
		//executor3 := tipb.Executor{
		//	Tp: tipb.ExecType_TypeAggregation,
		//	Aggregation: &tipb.Aggregation{
		//		AggFunc: []*tipb.Expr{aggExprPb},
		//		GroupBy: []*tipb.Expr{groupByExprPb},
		//	},
		//}
		//
		executor4 := tipb.Executor{
			Tp: tipb.ExecType_TypeLimit,
			Limit: &tipb.Limit{
				Limit: 2,
			},
		}

		executor5 := tipb.Executor{
			Tp: tipb.ExecType_TypeTopN,
			TopN: &tipb.TopN{
				Limit: 1,
				//OrderBy:
			},
		}

		dag.Executors = append(dag.Executors, &executor1, &executor2, &executor4, &executor5)
		full := ranger.FullIntRange(false)
		keyRange := distsql.TableRangesToKVRanges(tableInfo.ID, full, nil)
		copRange := &copRanges{mid: keyRange}
		data, _ := dag.Marshal()

		request := &tikvrpc.Request{
			Type: tikvrpc.CmdCop,
			Cop: &coprocessor.Request{
				Tp:     kv.ReqTypeDAG,
				Data:   data,
				Ranges: copRange.toPBRanges(),
			},

			Context: kvrpcpb.Context{
				//IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
				//Priority:       kvPriorityToCommandPri(worker.req.Priority),
				//NotFillCache:   worker.req.NotFillCache,
				HandleTime: true,
				ScanDetail: true,
			},
		}

		if e := tikvrpc.SetContext(request, rpcContext.Meta, rpcContext.Peer); e != nil {
			log.Fatal(e)
		}

		addr, err := c.loadStoreAddr(ctx, bo, peer.StoreId)
		if err != nil {
			log.Fatal(err)
		}
		tikvResp, err := c.RpcClient.SendRequest(ctx, addr, request, readTimeout)
		if err != nil {
			return err
		}
		resp := tikvResp.Cop
		if resp.RegionError != nil || resp.OtherError != "" {
			log.Fatal("coprocessor grpc call failed", resp.RegionError, resp.OtherError)
		}

		if resp.Data != nil {
			parseResponse(resp, getColumnsTypes(tableInfo.Columns))
		}
	}
	return nil
}

type selectResult struct {
	respChkIdx int
	fieldTypes []*types.FieldType

	selectResp *tipb.SelectResponse
	location   *time.Location
}

func parseResponse(resp *coprocessor.Response, fs []*types.FieldType) {
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

func decodeTableRow(row chunk.Row, fs []*types.FieldType) error {
	for i, f := range fs {
		switch {
		case f.Tp == mysql.TypeVarString || f.Tp == mysql.TypeVarchar:
			str := row.GetString(i)
			fmt.Printf("%s\t", str)
		case f.Tp == mysql.TypeLong:
			in := row.GetInt64(i)
			fmt.Printf("%d\t", in)
		}
	}
	fmt.Println()
	return nil
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

func getColumnsTypes(columns []*model.ColumnInfo) []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(columns))
	for _, col := range columns {
		colTypes = append(colTypes, &col.FieldType)
	}
	return colTypes
}

func ScanRowTable(ctx context.Context, c *ClusterClient) error {
	tableId := 123

	//rawData := []types.Datum{types.NewIntDatum(223), types.NewStringDatum("xxxabcdefg")}
	//timezone := time.UTC
	//
	//val, err := EncodeRowValue(rawData, timezone)
	//if err != nil {
	//	return err
	//}
	//
	//key := RecordKey(int64(tableId), 1)
	//err = c.Put(key, val)
	//if err != nil {
	//	return err
	//}

	// key1 val1 key2 val2 ...

	columns := []*model.ColumnInfo{
		{ID: 0, Offset: 0},
		{ID: 1, Offset: 1},
	}
	columns[0].Tp = mysql.TypeLong
	columns[0].Flen = types.UnspecifiedLength
	columns[0].Decimal = types.UnspecifiedLength

	columns[1].Tp = mysql.TypeVarchar
	columns[1].Flen = types.UnspecifiedLength
	columns[1].Decimal = types.UnspecifiedLength

	columnInfo := model.ColumnsToProto(columns, true)

	//ss, _ := session.CreateSession(c.Storage)
	//expr, _ := expression.ParseSimpleExprWithTableInfo(ss, "VARIABLE_NAME !='' ", tableInfo)
	//exprPb := expression.NewPBConverter(c.Storage.GetClient(), &stmtctx.StatementContext{InSelectStmt: true}).ExprToPB(expr)
	//executor2 := tipb.Executor{
	//	Tp: tipb.ExecType_TypeSelection,
	//	Selection: &tipb.Selection{
	//		Conditions: []*tipb.Expr{exprPb},
	//	},
	//}

	dag := &tipb.DAGRequest{
		StartTs:       math.MaxInt64,
		OutputOffsets: []uint32{0, 1},
		Executors: []*tipb.Executor{
			{
				Tp: tipb.ExecType_TypeTableScan,
				TblScan: &tipb.TableScan{
					TableId: 123,
					Columns: columnInfo,
					Desc:    false,
				},
			},

			{
				Tp: tipb.ExecType_TypeLimit,
				Limit: &tipb.Limit{
					Limit: 2,
				},
			},

			{
				Tp: tipb.ExecType_TypeTopN,
				TopN: &tipb.TopN{
					Limit: 1,
					//OrderBy:
				},
			},
		},
	}

	full := ranger.FullIntRange(false)
	keyRange := distsql.TableRangesToKVRanges(int64(tableId), full, nil)
	fmt.Printf("start key: %s\n", keyRange[0].StartKey)
	fmt.Printf("end key: %s\n", keyRange[0].EndKey)

	copRange := &copRanges{mid: keyRange}
	data, _ := dag.Marshal()
	request := &tikvrpc.Request{
		Type: tikvrpc.CmdCop,
		Cop: &coprocessor.Request{
			Tp:     kv.ReqTypeDAG,
			Data:   data,
			Ranges: copRange.toPBRanges(),
		},

		Context: kvrpcpb.Context{
			//IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
			//Priority:       kvPriorityToCommandPri(worker.req.Priority),
			//NotFillCache:   worker.req.NotFillCache,
			HandleTime: true,
			ScanDetail: true,
		},
	}

	regionInfa, er := c.GetRecordRegionIds(int64(tableId))

	if er != nil {
		log.Fatal(er)
	}

	log.Info("region id: ", regionInfa[0])

	bo := tikv.NewBackoffer(context.Background(), 20000)
	keyLocation, _ := c.RegionCache.LocateRegionByID(bo, regionInfa[0])

	rpcContext, _ := c.RegionCache.GetRPCContext(bo, keyLocation.Region)

	if e := tikvrpc.SetContext(request, rpcContext.Meta, rpcContext.Peer); e != nil {
		log.Fatal(e)
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
		log.Fatal("coprocessor grpc call failed", resp.RegionError, resp.OtherError)
	}

	if resp.Data != nil {
		parseResponse(resp, []*types.FieldType{types.NewFieldType(mysql.TypeLong), types.NewFieldType(mysql.TypeVarchar)})
	}
	return nil
}

func ScanRawIndex(ctx context.Context, c *ClusterClient) error {
	tableId := int64(123)
	indexId := int64(1)

	//rawData := []types.Datum{types.NewIntDatum(2345)}
	//timezone := time.UTC
	//rowId := int64(235)
	//val := []byte("235")
	//key, err := GenIndexKey(&stmtctx.StatementContext{TimeZone: timezone}, tableId, indexId, rawData, rowId)
	//err = c.Put(key, val)
	//if err != nil {
	//	return err
	//}

	columns := []*model.ColumnInfo{
		{ID: 0, Offset: 0},
		{ID: 1, Offset: 1},
	}
	columns[0].Tp = mysql.TypeLong
	columns[0].Flen = types.UnspecifiedLength
	columns[0].Decimal = types.UnspecifiedLength

	columns[1].Tp = mysql.TypeLong
	columns[1].Flen = types.UnspecifiedLength
	columns[1].Decimal = types.UnspecifiedLength

	uni := true
	dag := &tipb.DAGRequest{
		StartTs:       math.MaxInt64,
		OutputOffsets: []uint32{0, 1},
		Executors: []*tipb.Executor{
			{
				Tp: tipb.ExecType_TypeIndexScan,
				IdxScan: &tipb.IndexScan{
					TableId: tableId,
					IndexId: indexId,
					Columns: model.ColumnsToProto(columns, true),
					Unique:  &uni,
					Desc:    false},
			},
		},
	}

	full := ranger.FullIntRange(false)
	keyRange, _ := distsql.IndexRangesToKVRanges(&stmtctx.StatementContext{InSelectStmt: true}, tableId, indexId, full, nil)
	copRange := &copRanges{mid: keyRange}
	data, _ := dag.Marshal()

	request := &tikvrpc.Request{
		Type: tikvrpc.CmdCop,
		Cop: &coprocessor.Request{
			Tp:     kv.ReqTypeDAG,
			Data:   data,
			Ranges: copRange.toPBRanges(),
		},

		Context: kvrpcpb.Context{
			//IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
			//Priority:       kvPriorityToCommandPri(worker.req.Priority),
			//NotFillCache:   worker.req.NotFillCache,
			HandleTime: true,
			ScanDetail: true,
		},
	}

	regionInfa, er := c.GetIndexRegionIds(int64(tableId), int64(1))

	if er != nil {
		log.Fatal(er)
	}
	log.Info("region id: ", regionInfa[0])

	bo := tikv.NewBackoffer(context.Background(), 20000)
	keyLocation, _ := c.RegionCache.LocateRegionByID(bo, regionInfa[0])

	rpcContext, _ := c.RegionCache.GetRPCContext(bo, keyLocation.Region)

	if e := tikvrpc.SetContext(request, rpcContext.Meta, rpcContext.Peer); e != nil {
		log.Fatal(e)
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
		log.Fatal("coprocessor grpc call failed", resp.RegionError, resp.OtherError)
	}

	if resp.Data != nil {
		parseResponse(resp, []*types.FieldType{types.NewFieldType(mysql.TypeLong), types.NewFieldType(mysql.TypeLong)})
	}
	return nil
}

// RecordKey implements table.Table interface.
func RecordKey(tableId, h int64) kv.Key {
	return tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(tableId), h)
}

func EncodeRowValue(rawData []types.Datum, timezone *time.Location) ([]byte, error) {
	values := make([]types.Datum, len(rawData)*2)
	for idex, v := range rawData {
		values[2*idex].SetInt64(int64(idex))
		err := flatten(v, &values[2*idex+1], timezone)
		if err != nil {
			return nil, err
		}
	}
	var valBuf []byte
	return codec.EncodeValue(&stmtctx.StatementContext{TimeZone: timezone}, valBuf, values...)
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func GenIndexKey(sc *stmtctx.StatementContext, tableId int64, indexId int64, indexedValues []types.Datum, rowId int64) ([]byte, error) {
	//if c.idxInfo.Unique {
	//	// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
	//	// A UNIQUE index creates a constraint such that all values in the index must be distinct.
	//	// An error occurs if you try to add a new row with a key value that matches an existing row.
	//	// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
	//	distinct = true
	//	for _, cv := range indexedValues {
	//		if cv.IsNull() {
	//			distinct = false
	//			break
	//		}
	//	}
	//}

	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	//indexedValues = TruncateIndexValuesIfNeeded(c.tblInfo, c.idxInfo, indexedValues)
	var key []byte
	//key = c.getIndexKeyBuf(buf, len(c.prefix)+len(indexedValues)*9+9)
	key = append(key, []byte(tablecodec.EncodeTableIndexPrefix(tableId, indexId))...)
	key, err := codec.EncodeKey(sc, key, indexedValues...)
	if err == nil {
		key, err = codec.EncodeKey(sc, key, types.NewDatum(rowId))
	}
	return key, err
}

func flatten(data types.Datum, ret *types.Datum, timezone *time.Location) error {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type == mysql.TypeTimestamp && timezone != time.UTC {
			err := t.ConvertTimeZone(timezone, time.UTC)
			if err != nil {
				return err
			}
		}
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return err
	case types.KindMysqlDuration:
		// for mysql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.KindMysqlEnum:
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.KindMysqlSet:
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since thenilnil literal is ensured to be able to store in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(nil)
		if err != nil {
			return err
		}
		ret.SetUint64(val)
		return nil
	default:
		*ret = data
		return nil
	}
}
