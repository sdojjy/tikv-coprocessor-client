package coprocessor

import (
	"fmt"
	"github.com/sdojjy/tikv-coprocessor-client/config"
	"github.com/sdojjy/tikv-coprocessor-client/rawkv"
	"testing"
)

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
