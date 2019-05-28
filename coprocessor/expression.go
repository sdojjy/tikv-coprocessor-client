package coprocessor

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *CopClient) ParseExprWithTableInfo(tableInfo *model.TableInfo, exprStr string) (*tipb.Expr, error) {
	ss, err := session.CreateSession(c.Storage)
	if err != nil {
		return nil, err
	}
	expr, err := expression.ParseSimpleExprWithTableInfo(ss, exprStr, tableInfo)
	if err != nil {
		return nil, err
	}
	return expression.NewPBConverter(c.Storage.GetClient(), &stmtctx.StatementContext{InSelectStmt: true}).ExprToPB(expr), nil
}

func (c *CopClient) ParseExpress(tableInfo *MockTableInfo, exprStr string) (*tipb.Expr, error) {
	return c.ParseExprWithTableInfo(tableInfo.ToInnerTableInfo(), exprStr)
}

func (c *CopClient) GenAggExprPB(name string, args []expression.Expression, hasDistinct bool) (*tipb.Expr, error) {
	ss, err := session.CreateSession(c.Storage)
	if err != nil {
		panic(err)
	}
	agg := aggregation.NewAggFuncDesc(ss, name, args, hasDistinct)
	return aggregation.AggFuncToPBExpr(&stmtctx.StatementContext{InSelectStmt: true}, c.Storage.GetClient(), agg), nil
}

func (c *CopClient) GetGroupByPB(expr expression.Expression) *tipb.Expr {
	return expression.GroupByItemToPB(&stmtctx.StatementContext{InSelectStmt: true}, c.Storage.GetClient(), expr).Expr
}
