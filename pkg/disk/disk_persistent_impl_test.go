package disk

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func randomPath() string {
	bs := make([]byte, 8)
	_, _ = rand.Read(bs)
	filename := hex.EncodeToString(bs)
	return path.Join(os.TempDir(), filename)
}

func removePath(path string) error {
	return os.RemoveAll(path)
}

func TestFilePersistentImpl_WriteReadDisk(t *testing.T) {
	p := randomPath()
	defer func() {
		err := removePath(p)
		require.NoError(t, err)
	}()
	di, err := NewFilePersistentImpl(p, 0)
	require.NoError(t, err)
	require.NotNil(t, di)
	// 先写
	value := []byte("hello disk")
	valLen := len(value)
	offset, wn, err := di.WriteToDisk(value)
	require.NoError(t, err)
	require.Equal(t, valLen, wn)
	require.Equal(t, int64(0), offset)
	gotValue := make([]byte, valLen)
	rn, err := di.ReadFromDisk(gotValue, 0)
	require.NoError(t, err)
	require.Equal(t, valLen, rn)
	require.Equal(t, value, gotValue)

	// 再写
	value2 := []byte("hello disk again")
	val2Len := len(value2)
	offset, wn, err = di.WriteToDisk(value2)
	require.NoError(t, err)
	require.Equal(t, val2Len, wn)
	require.Equal(t, int64(valLen), offset)
	gotValue2 := make([]byte, val2Len)
	rn, err = di.ReadFromDisk(gotValue2, uint64(valLen))
	require.NoError(t, err)
	require.Equal(t, val2Len, rn)
	require.Equal(t, value2, gotValue2)
}
