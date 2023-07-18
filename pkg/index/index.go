package index

import (
	"errors"
)

var ErrKeyIsNil = errors.New("key is nil")

// ValueMetadata Indexer 中key对应的Value的结构
type ValueMetadata struct {
	// 文件ID
	FileID uint64
	// key对应的value的长度,value包含了crc,tmstamp,ksz,value_sz,key,value
	ValueSz uint64
	// key对应的value在file中的起始位置位置
	ValuePos uint64
	// 时间戳
	TsTamp int64
}

func NewValueMetadata(fileID uint64, valueSz uint64, valuePos uint64, ts int64) *ValueMetadata {
	return &ValueMetadata{FileID: fileID, ValueSz: valueSz, ValuePos: valuePos, TsTamp: ts}
}

// Indexer - 内存索引接口
type Indexer interface {
	// Get - get index value by key
	Get(key []byte) (*ValueMetadata, error)
	// Set - set index value by key
	Set(key []byte, value *ValueMetadata) error
	// Del - delete index value by key
	Del(key []byte) error
}

func checkKey(k []byte) error {
	if k == nil {
		return ErrKeyIsNil
	}
	return nil
}
