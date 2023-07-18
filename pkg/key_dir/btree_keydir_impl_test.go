package key_dir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBtree_Set(t *testing.T) {
	br := NewBtree()
	err := br.Set([]byte("key1"), NewMemValue(1, 1, 1))
	require.NoError(t, err)
	err = br.Set([]byte("key2"), NewMemValue(2, 2, 2))
	require.NoError(t, err)
	err = br.Set(nil, NewMemValue(3, 3, 3))
	require.Error(t, err)
	err = br.Set([]byte(""), NewMemValue(4, 4, 4))
	require.NoError(t, err)
	v4, err := br.Get([]byte(""))
	require.NoError(t, err)
	require.Equal(t, uint64(4), v4.FileID)
	require.Equal(t, uint64(4), v4.ValueSz)
	require.Equal(t, uint64(4), v4.ValuePos)

}

func TestBtree_Get(t *testing.T) {
	br := NewBtree()
	err := br.Set([]byte("key1"), NewMemValue(1, 1, 1))
	require.NoError(t, err)
	v1, err := br.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), v1.FileID)
	require.Equal(t, uint64(1), v1.ValueSz)
	require.Equal(t, uint64(1), v1.ValuePos)
	err = br.Set([]byte("key1"), NewMemValue(2, 2, 2))
	require.NoError(t, err)
	v2, err := br.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), v2.FileID)
	require.Equal(t, uint64(2), v2.ValueSz)
	require.Equal(t, uint64(2), v2.ValuePos)

	vNil, err := br.Get(nil)
	require.Error(t, err)
	require.Nil(t, vNil)
	vNotExist, err := br.Get([]byte("keyNotExist"))
	require.NoError(t, err)
	require.Nil(t, vNotExist)

	// test empty key("")
	err = br.Set([]byte(""), NewMemValue(3, 3, 3))
	require.NoError(t, err)
	v3, err := br.Get([]byte(""))
	require.NoError(t, err)
	require.Equal(t, uint64(3), v3.FileID)
	require.Equal(t, uint64(3), v3.ValueSz)
	require.Equal(t, uint64(3), v3.ValuePos)
}

func TestBtree_Del(t *testing.T) {
	br := NewBtree()
	err := br.Set([]byte("key1"), NewMemValue(1, 1, 1))
	require.NoError(t, err)
	k1Val, err := br.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), k1Val.FileID)
	require.Equal(t, uint64(1), k1Val.ValueSz)
	require.Equal(t, uint64(1), k1Val.ValuePos)
	err = br.Del([]byte("key1"))
	require.NoError(t, err)
	k1Val, err = br.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, k1Val)
	// delete not exist item
	err = br.Del([]byte("keyNotExist"))
	require.NoError(t, err)

	// delete nil
	err = br.Del(nil)
	require.Error(t, err)

	// delete empty key
	err = br.Del([]byte(""))
	require.NoError(t, err)
}
