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

func TestDB_AllOption(t *testing.T) {
	opts := NewOptions([]OptionsFunc{
		MaxSizeOption(0),
		DirOption("./db"),
		AlwaysSyncOption(true),
	})
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

	// del
	err = db.Del([]byte("key1"))
	require.NoError(t, err)
	err = db.Del([]byte("keyNotExist"))
	require.NoError(t, err)
	gotV, err := db.Get([]byte("keyNotExist"))
	require.NoError(t, err)
	require.Nil(t, gotV)

	err = db.Close()
	require.NoError(t, err)
	err = db.activeFile.Delete()
	for _, f := range db.oldFiles {
		err = f.Delete()
		require.NoError(t, err)
	}
	require.NoError(t, err)
}
