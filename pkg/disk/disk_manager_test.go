package disk

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Manager(t *testing.T) {

	var (
		k1   = []byte("key")
		v1   = []byte("value")
		reV1 = []byte("revalue")

		k2 = []byte("")
		v2 = []byte("empty")
	)
	m, err := NewManager(path.Join(os.TempDir(), "test"), 0)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	// write
	keyDir1, err := m.Write(k1, v1)
	assert.NoError(t, err)
	assert.NotNil(t, keyDir1)
	assert.Equal(t, uint64(0), keyDir1.FileID)
	// 新的文件第一次写入，偏移肯定是0
	assert.Equal(t, uint64(0), keyDir1.ValuePos)
	assert.NotZero(t, keyDir1.TsTamp)

	gotV1, err := m.Read(keyDir1)
	assert.NoError(t, err)
	assert.Equal(t, v1, gotV1)

	// rewrite
	keyDirRe1, err := m.Write(k1, reV1)
	assert.NoError(t, err)
	assert.NotNil(t, keyDirRe1)
	assert.Greater(t, keyDirRe1.ValuePos, keyDir1.ValuePos)
	assert.Equal(t, keyDirRe1.ValuePos, keyDir1.ValuePos+keyDir1.ValueSz)
	gotV1re, err := m.Read(keyDirRe1)
	assert.NoError(t, err)
	assert.Equal(t, reV1, gotV1re)

	// test write empty key
	keyDir2, err := m.Write(k2, v2)
	assert.NoError(t, err)
	assert.NotNil(t, keyDir2)
	assert.Equal(t, keyDir2.ValuePos, keyDirRe1.ValuePos+keyDirRe1.ValueSz)
	gotV2, err := m.Read(keyDir2)
	assert.NoError(t, err)
	assert.Equal(t, v2, gotV2)

	// close and delete
	defer func() {
		err = m.Close()
		require.NoError(t, err)
		err = m.Delete()
		require.NoError(t, err)
	}()

}
