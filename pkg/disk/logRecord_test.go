package disk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_All(t *testing.T) {

	var (
		k = []byte("hello")
		v = []byte("world")
	)
	normalLogRecord, err := NewNormalLogRecord(k, v)
	require.NoError(t, err)
	crcInput, err := normalLogRecord.crcData()
	require.NoError(t, err)
	require.NotNil(t, crcInput)
	normalLogRecord.calCrc(crcInput)

	// write to disk
	bs, err := normalLogRecord.Serialize()
	require.NoError(t, err)
	require.NotNil(t, bs)

	// read from disk
	// 感觉是一个函数或者静态方法更自然
	gotNormalLogRecord, err := normalLogRecord.Deserialize(bs)
	require.NoError(t, err)
	require.NotNil(t, gotNormalLogRecord)
	require.Equal(t, normalLogRecord, gotNormalLogRecord)

	// delete
	deleteLogRecord, err := NewDeleteLogRecord(k)
	require.NoError(t, err)
	crcInput, err = deleteLogRecord.crcData()
	require.NoError(t, err)
	require.NotNil(t, crcInput)
	deleteLogRecord.calCrc(crcInput)

	// write to disk
	ds, err := deleteLogRecord.Serialize()
	require.NoError(t, err)
	require.NotNil(t, ds)

	// read from disk
	gotDeleteLogRecord, err := deleteLogRecord.Deserialize(ds)
	require.NoError(t, err)
	require.NotNil(t, gotDeleteLogRecord)
	require.Equal(t, deleteLogRecord, gotDeleteLogRecord)

}
