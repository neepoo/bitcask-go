package key_dir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap_Del(t *testing.T) {
	m := NewMap()
	err := m.Set([]byte("key1"), NewMemValue(1, 1, 1))
	require.NoError(t, err)
	k1Val, err := m.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), k1Val.FileID)
	require.Equal(t, uint64(1), k1Val.ValueSz)
	require.Equal(t, uint64(1), k1Val.ValuePos)
	err = m.Del([]byte("key1"))
	require.NoError(t, err)
	k1Val, err = m.Get([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, k1Val)
	// delete not exist item
	err = m.Del([]byte("keyNotExist"))
	require.NoError(t, err)

	// delete nil
	err = m.Del(nil)
	require.Error(t, err)

	// delete empty key
	err = m.Del([]byte(""))
	require.NoError(t, err)
}

func TestMap_Get(t *testing.T) {
	m := NewMap()
	err := m.Set([]byte("key1"), NewMemValue(1, 1, 1))
	require.NoError(t, err)
	v1, err := m.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), v1.FileID)
	require.Equal(t, uint64(1), v1.ValueSz)
	require.Equal(t, uint64(1), v1.ValuePos)
	err = m.Set([]byte("key1"), NewMemValue(2, 2, 2))
	require.NoError(t, err)
	v2, err := m.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), v2.FileID)
	require.Equal(t, uint64(2), v2.ValueSz)
	require.Equal(t, uint64(2), v2.ValuePos)

	vNil, err := m.Get(nil)
	require.Error(t, err)
	require.Nil(t, vNil)
	vNotExist, err := m.Get([]byte("keyNotExist"))
	require.NoError(t, err)
	require.Nil(t, vNotExist)

	// test empty key("")
	err = m.Set([]byte(""), NewMemValue(3, 3, 3))
	require.NoError(t, err)
	v3, err := m.Get([]byte(""))
	require.NoError(t, err)
	require.Equal(t, uint64(3), v3.FileID)
	require.Equal(t, uint64(3), v3.ValueSz)
	require.Equal(t, uint64(3), v3.ValuePos)
}

func TestMap_Set(t *testing.T) {
	m := NewMap()
	err := m.Set([]byte("key1"), NewMemValue(1, 1, 1))
	require.NoError(t, err)
	err = m.Set([]byte("key2"), NewMemValue(2, 2, 2))
	require.NoError(t, err)
	err = m.Set(nil, NewMemValue(3, 3, 3))
	require.Error(t, err)
	err = m.Set([]byte(""), NewMemValue(4, 4, 4))
	require.NoError(t, err)
	v4, err := m.Get([]byte(""))
	require.NoError(t, err)
	require.Equal(t, uint64(4), v4.FileID)
	require.Equal(t, uint64(4), v4.ValueSz)
	require.Equal(t, uint64(4), v4.ValuePos)
}
