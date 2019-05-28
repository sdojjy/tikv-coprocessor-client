package coprocessor

import (
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/prometheus/common/log"
)

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

func printDatum(values [][]types.Datum) {
	for _, row := range values {
		for _, col := range row {
			str, _ := col.ToString()
			fmt.Printf("%s\t", str)
		}
		fmt.Println()
	}
}
