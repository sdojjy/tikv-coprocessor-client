package coprocessor

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *ClusterClient) ParseExpress(tableInfo *TableInfo, exprStr string) (*tipb.Expr, error) {
	ss, err := session.CreateSession(c.Storage)
	if err != nil {
		return nil, err
	}
	expr, err := expression.ParseSimpleExprWithTableInfo(ss, exprStr, tableInfo.ToInnerTableInfo())
	if err != nil {
		return nil, err
	}
	return expression.NewPBConverter(c.Storage.GetClient(), &stmtctx.StatementContext{InSelectStmt: true}).ExprToPB(expr), nil
}

func (c *ClusterClient) GenAggExprPB(name string, args []expression.Expression, hasDistinct bool) (*tipb.Expr, error) {
	ss, err := session.CreateSession(c.Storage)
	if err != nil {
		panic(err)
	}
	agg := aggregation.NewAggFuncDesc(ss, ast.AggFuncCount, args, hasDistinct)
	return aggregation.AggFuncToPBExpr(&stmtctx.StatementContext{InSelectStmt: true}, c.Storage.GetClient(), agg), nil
}

func (c *ClusterClient) GetGroupByPB(expr expression.Expression) *tipb.Expr {
	return expression.GroupByItemToPB(&stmtctx.StatementContext{InSelectStmt: true}, c.Storage.GetClient(), expr).Expr
}
