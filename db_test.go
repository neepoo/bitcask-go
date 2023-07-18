package bitcast_go

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_All(t *testing.T) {
	opts := NewDefaultOptions()
	db := NewDb(opts)
	err := db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	val, err := db.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
	notExistVal, err := db.Get([]byte("keyNotExist"))
	require.NoError(t, err)
	require.Nil(t, notExistVal)

	err = db.Put([]byte("key1"), []byte("value2"))
	require.NoError(t, err)
	val, err = db.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), val)
	err = db.Close()
	require.NoError(t, err)
	err = db.activeFile.Delete()
	require.NoError(t, err)
}
