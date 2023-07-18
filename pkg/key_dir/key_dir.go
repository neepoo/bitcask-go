package key_dir

import "time"

// MemValue keydir 中key对应的Value的结构
type MemValue struct {
	// 文件ID
	FileID uint64
	// key对应的value的长度,value包含了crc,tmstamp,ksz,value_sz,key,value
	ValueSz uint64
	// key对应的value在file中的起始位置位置
	ValuePos uint64
	// 时间戳
	TsTamp int64
}

func NewMemValue(fileID uint64, valueSz uint64, valuePos uint64) *MemValue {
	ts := time.Now().Unix()
	return &MemValue{FileID: fileID, ValueSz: valueSz, ValuePos: valuePos, TsTamp: ts}
}
