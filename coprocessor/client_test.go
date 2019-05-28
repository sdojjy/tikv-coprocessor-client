package coprocessor

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/sdojjy/tikv-coprocessor-client/config"
	"github.com/sdojjy/tikv-coprocessor-client/rawkv"
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

func TestClusterClient_Put(t *testing.T) {
	getClient(t).Put([]byte("company"), []byte("pingcap"))
}

func TestGet(t *testing.T) {
	cli, err := rawkv.NewClient([]string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	key := GenRecordKey(123, 2)
	val, err := cli.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found val: %s for key: %s\n", val, key)
}

func Test_range(t *testing.T) {
	full := ranger.FullRange()
	dbInfo, _ := getClient(t).GetTableInfo("mysql", "user")
	kv := distsql.TableRangesToKVRanges(dbInfo.ID, full, nil)
	fmt.Printf("%v", kv)
}
