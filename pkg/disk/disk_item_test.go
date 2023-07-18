package disk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_All(t *testing.T) {
	var (
		key   = []byte("key")
		value = []byte("value")
	)
	var defaultItem = &Item{
		tmStamp: uint64(time.Now().Unix()),
		ksz:     uint64(len(key)),
		valueSz: uint64(len(value)),
		key:     key,
		value:   value,
	}
	crcInput, err := defaultItem.CrcData()
	require.NoError(t, err)
	require.NotNil(t, crcInput)
	defaultItem.CalCrc(crcInput)

	// write to disk
	bs, err := defaultItem.Serialize()
	require.NoError(t, err)
	require.NotNil(t, bs)

	// read from disk
	// 感觉是一个函数或者静态方法更自然
	item, err := defaultItem.Deserialize(bs)
	require.NoError(t, err)
	require.NotNil(t, item)
	require.Equal(t, defaultItem, item)
}
