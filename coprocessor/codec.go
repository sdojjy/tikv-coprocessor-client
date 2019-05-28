package coprocessor

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"time"
)

func EncodeRowValue(rawData []types.Datum, colIndex []int, timezone *time.Location) ([]byte, error) {
	values := make([]types.Datum, len(rawData)*2)
	for index, v := range rawData {
		values[2*index].SetInt64(int64(colIndex[index]))
		err := flatten(v, &values[2*index+1], timezone)
		if err != nil {
			return nil, err
		}
	}
	var valBuf []byte
	return codec.EncodeValue(&stmtctx.StatementContext{TimeZone: timezone}, valBuf, values...)
}

// GenRecordKey implements table.Table interface.
func GenRecordKey(tableId, rowId int64) kv.Key {
	return tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(tableId), rowId)
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func GenIndexKey(sc *stmtctx.StatementContext, tableId int64, indexId int64, indexedValues []types.Datum, rowId int64, unique bool) ([]byte, error) {
	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	//indexedValues = TruncateIndexValuesIfNeeded(c.tblInfo, c.idxInfo, indexedValues)
	var key []byte
	key = append(key, []byte(tablecodec.EncodeTableIndexPrefix(tableId, indexId))...)
	key, err := codec.EncodeKey(sc, key, indexedValues...)
	if err == nil && !unique {
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
