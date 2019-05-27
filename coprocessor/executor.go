package coprocessor

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tipb/go-tipb"
)

func NewTopNExecutor(limit uint64, orderBy []*tipb.ByItem) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeTopN,
		TopN: &tipb.TopN{
			Limit:   limit,
			OrderBy: orderBy,
		},
	}
}

func NewLimitExecutor(limit uint64) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeLimit,
		Limit: &tipb.Limit{
			Limit: limit,
		},
	}
}

func NewTableScanExecutor(tableInfo *TableInfo, desc bool) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeTableScan,
		TblScan: &tipb.TableScan{
			TableId: tableInfo.ID,
			Columns: model.ColumnsToProto(tableInfo.GetColumnInfo(), false),
			Desc:    desc,
		},
	}
}

func NewTableScanExecutorWithTypes(tableId int64, types []*types.FieldType, desc bool) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeTableScan,
		TblScan: &tipb.TableScan{
			TableId: tableId,
			Columns: model.ColumnsToProto(TypesToColumnInfo(types), false),
			Desc:    desc,
		},
	}
}

func NewIndexScanExecutor(tableInfo *TableInfo, indexId int64, desc bool) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeIndexScan,
		IdxScan: &tipb.IndexScan{
			TableId: tableInfo.ID,
			IndexId: indexId,
			Columns: model.ColumnsToProto(tableInfo.GetColumnInfo(), false),
			Desc:    desc},
	}
}

func NewSelectionScanExecutor(conditions []*tipb.Expr) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeSelection,
		Selection: &tipb.Selection{
			Conditions: conditions,
		},
	}
}

func NewAggregationExecutor(agg []*tipb.Expr, groupBy []*tipb.Expr) *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeAggregation,
		Aggregation: &tipb.Aggregation{
			GroupBy:  groupBy,
			AggFunc:  agg,
			Streamed: false,
		},
	}
}
