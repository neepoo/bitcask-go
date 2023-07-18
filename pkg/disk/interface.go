package disk

import "bitcask-go/pkg/index"

// DataSerializer 数据序列化接口
// 用于内存和磁盘之间的数据序列化,类似ddd中的anti-corruption layer
type DataSerializer interface {
	// Serialize 返回需要写往磁盘的数据
	Serialize() ([]byte, error)
	// Deserialize 从磁盘读取数据的数据反序列化成内存数据结构
	Deserialize([]byte) (DataSerializer, error)
	// Op 返回当前数据的操作类型 normal or delete
	Op() LogRecordType
	// Value 返回具体的值
	Value() []byte
}

// PersistentStorage 持久化存储接口
// 配合DataSerializer接口一起给业务方提供持久化能力
// 读的流程，PersistentStorage.ReadFromDisk -> DataSerializer.Deserialize
// 写的流程，DataSerializer.Serialize -> PersistentStorage.WriteToDisk
type PersistentStorage interface {
	// ReadFromDisk 从文件的offset处读取len(bs)的数据到bs中，返回读取的字节数和错误
	// NOTE: 这里潜在的性能问题,如果bs很大，每次拷贝的成本可能就无法忽视，应该使用指针 bs *[]byte
	ReadFromDisk(bs []byte, offset uint64) (int, error)
	// WriteToDisk 将bs追加写往文件，返回写入的起始位置,写入的字节数和错误
	WriteToDisk(bs []byte) (offset int64, wn int, err error)
	// Sync 同步数据到磁盘
	Sync() error
	// Close 关闭文件
	Close() error
	// Delete 删除文件
	Delete() error
	// Offset 返回当前文件的的写入位置
	Offset() int64
}

// DataFile 磁盘文件的表示, Write和Del操作都是追加写，不会覆盖，只作用于当前活跃文件(active data file). Read则是什么类型的DataFile都支持
type DataFile interface {
	// Write 将key和value写入磁盘，返回dirkey需要的结构
	Write(key, value []byte, force bool) (v *index.ValueMetadata, err error)
	// Read 从磁盘读取key对应的value
	Read(mv *index.ValueMetadata) (value []byte, err error)
	// Del 删除key对应的value
	Del(key []byte, force bool) (v *index.ValueMetadata, err error)
	// ID 返回文件ID，对应index中的file_id
	ID() uint64 // 文件ID，对应index中的file_id
	// Close 关闭文件
	Close() error
	// Delete 删除文件
	Delete() error
	// ToOlderFile 将活跃的data file转换成older data file
	ToOlderFile() (DataFile, error)
}
