package coprocessor

import (
	"fmt"
	"testing"
)

func TestGetTableInfo(t *testing.T) {
	dbInfo, err := getClient(t).GetTableInfo("mysql", "tidb")
	fmt.Printf("%v, %v", dbInfo, err)
}
