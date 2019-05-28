# tikv-coprocessor-client


This client is based on [client-go](https://github.com/tikv/client-go), it's designed to test tikv coprocessor directly.

## Feature

- Insert table record and index record into tikv using tikv grpc interface.
- Send coprocessor request to tikv and parse the response.



## Init the client
To init the coprocessor client we need the pd address.
```go
client, err := coprocessor.NewClient([]string{"127.0.0.1:2379"}, config.Security{})
if err != nil {
	t.Fatal("create client failed")
}
```

## Insert data to tikv
Before you insert the data, you need a table id and index id, you can assign it manually or get it by calling [CopClient.GetTableInfo](coprocessor/table.go) method
Examples see: [record_test](./coprocessor/record_test.go) 

## Send Coprocessor Request
Check comment in [cop](coprocessor/cop.go) file, that file defined a lot methods you can call to send coprocessor request.
Example: see [cop_test](coprocessor/cop_test.go)

## Examples

- Insert data to a table with index.
```go
package main

import (
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/prometheus/common/log"
	"github.com/sdojjy/tikv-coprocessor-client/coprocessor"
)

func main() {
	client, err := coprocessor.NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	//create table t2 (id int , name varchar(25), key `name` (`name`));
	tableInfo, _ := client.GetTableInfo("test", "t2")
	rawData := []types.Datum{types.NewIntDatum(1), types.NewStringDatum("jerry")}
	log.Info(client.AddTableRecord(tableInfo.ID, 1, rawData))

	indexData := []types.Datum{types.NewStringDatum("jerry")}
	log.Info(client.AddIndexRecord(tableInfo.ID, tableInfo.Indices[0].ID, 1, indexData, false))
	//result
	/**
	  mysql>  select name from t2 where name in ('jerry','ttt');
	  +-------+
	  | name  |
	  +-------+
	  | jerry |
	  +-------+
	*/
}

```

- Send table scan with conditions

```go

package main

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/prometheus/common/log"
	"github.com/sdojjy/tikv-coprocessor-client/coprocessor"
)

func main() {
	c, err := coprocessor.NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	//create table t2 (id int , name varchar(25), key `name` (`name`));
	tableInfo, _ := c.GetTableInfo("test", "t2")
	ret, _ := c.ScanTableWithConditionsAndTableInfo(context.Background(), tableInfo)
	printDatum(ret)
	fmt.Println("mock table info scan")
	ret, _ = c.ScanTableWithConditions(context.Background(), coprocessor.InnerTableInfoToMockTableInfo(tableInfo), "id!=1")
	printDatum(ret)
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

```

- Send Coprocessor executors
```go
    ... 
	agg, err := c.GenAggExprPB(ast.AggFuncMax, []expression.Expression{col}, false)
	if err != nil {
		log.Fatal(err)
	}

	returnTypes := []*types.FieldType{types.NewFieldType(mysql.TypeFloat), types.NewFieldType(mysql.TypeVarchar)}
	executors := []*tipb.Executor{
		NewTableScanExecutorWithTypes(tableInfo.ID, tableInfo.Types, false),
		NewSelectionScanExecutor([]*tipb.Expr{expr}),
		NewAggregationExecutor([]*tipb.Expr{agg}, []*tipb.Expr{c.GetGroupByPB(groupById)}),
		NewLimitExecutor(2),
	}
	...
	c.SendCoprocessorRequest(context.Background(), tableInfo.ID, returnTypes, executors, rangeFunc, decodeTableRow)
```