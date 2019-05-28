package coprocessor

import (
	"github.com/pingcap/tidb/types"
	"testing"
)

func TestAddTableRecord(t *testing.T) {
	c := getClient(t)
	//create table t (id int , name varchar(25));
	tableInfo, _ := c.GetTableInfo("test", "t")
	rawData := []types.Datum{types.NewIntDatum(1), types.NewStringDatum("xxxabcdefg")}

	c.AddTableRecord(tableInfo.ID, 1, rawData)
	c.AddTableRecord(tableInfo.ID, 2, rawData)
	//result:
	/**
	mysql> select * from t;
	+------+------------+
	| id   | name       |
	+------+------------+
	|    1 | xxxabcdefg |
	|    1 | xxxabcdefg |
	+------+------------+
	*/
}

func TestAddIndexRecord(t *testing.T) {
	c := getClient(t)
	defer c.Close()
	//create table t2 (id int , name varchar(25), key `name` (`name`));
	tableInfo, _ := c.GetTableInfo("test", "t2")
	rawData := []types.Datum{types.NewIntDatum(1), types.NewStringDatum("jerry")}
	c.AddTableRecord(tableInfo.ID, 1, rawData)

	indexData := []types.Datum{types.NewStringDatum("jerry")}
	c.AddIndexRecord(tableInfo.ID, tableInfo.Indices[0].ID, 1, indexData, false)
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
