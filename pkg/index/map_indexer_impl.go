package index

import (
	"encoding/hex"
	"sync"
)

type Map struct {
	m sync.Map
}

func NewMap() Indexer {
	return &Map{
		m: sync.Map{},
	}
}

func byteToString(key []byte) string {
	return hex.EncodeToString(key)
}

func (m *Map) Get(key []byte) (*ValueMetadata, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	strKey := byteToString(key)
	v, ok := m.m.Load(strKey)
	if !ok {
		return nil, nil
	}
	return v.(*ValueMetadata), nil
}

func (m *Map) Set(key []byte, value *ValueMetadata) error {
	if err := checkKey(key); err != nil {
		return err
	}
	// []byte is unhashable
	strKey := byteToString(key)
	m.m.Store(strKey, value)
	return nil
}

func (m *Map) Del(key []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	strKey := byteToString(key)
	m.m.Delete(strKey)
	return nil
}
