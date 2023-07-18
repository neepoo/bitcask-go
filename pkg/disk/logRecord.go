package disk

/*
如果是LogRecord类型是NormalRecord，那么磁盘存储会包含LogRecord的所有字段
如果是LogRecord类型是DeleteRecord，那么磁盘存储只包含LogRecord的crc、typ、tmStamp、ksz、key字段
*/

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"time"
)

type LogRecordType uint8

const (
	// NormalRecord 表示普通的记录
	NormalRecord LogRecordType = iota
	// DeleteRecord 表示删除记录
	DeleteRecord

	crcSz     = 4 // uint32
	tmStampSz = 8 // uint64
	kszSz     = 8 // uint64
	vszSz     = 8 // uint64
	typeSz    = 1 // uint8
)

var (
	// 默认的字节序，小端序
	defaultEndianness = binary.LittleEndian
	ErrCrcCheckFailed = errors.New("crc check failed")
	bfPool            = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
)

// LogRecord 表示bitcask磁盘文件中的一个条目
// 完全按照bitcask的格式存储
type LogRecord struct {
	// crc校验码
	crc uint32
	// 类型 normal or delete
	typ LogRecordType
	// 时间戳
	tmStamp uint64
	// key的长度
	ksz uint64
	// value的长度
	valueSz uint64
	// key
	key []byte
	// value
	value []byte
}

// NewNormalLogRecord 创建一个普通的LogRecord
// set时使用
func NewNormalLogRecord(k, v []byte) (*LogRecord, error) {
	res := new(LogRecord)
	res.key = k
	res.value = v
	res.tmStamp = uint64(time.Now().Unix())
	res.ksz = uint64(len(k))
	res.valueSz = uint64(len(v))
	res.typ = NormalRecord
	crcInput, err := res.crcData()
	if err != nil {
		return nil, err
	}
	res.calCrc(crcInput)
	return res, nil
}

// NewDeleteLogRecord 创建一个删除的LogRecord
// delete时使用
func NewDeleteLogRecord(k []byte) (*LogRecord, error) {
	res := new(LogRecord)
	res.key = k
	res.tmStamp = uint64(time.Now().Unix())
	res.ksz = uint64(len(k))
	res.typ = DeleteRecord
	crcInput, err := res.crcData()
	if err != nil {
		return nil, err
	}
	res.calCrc(crcInput)
	return res, nil
}

// Size 返回LogRecord的大小
func (d *LogRecord) Size() int64 {
	switch d.typ {
	case NormalRecord:
		return int64(crcSz + typeSz + tmStampSz + kszSz + vszSz + len(d.key) + len(d.value))
	case DeleteRecord:
		return int64(crcSz + typeSz + tmStampSz + kszSz + len(d.key))
	default:
		// dead code
		return 0
	}
}

// crcData 得到计算crc的输入数据，不同类型的LogRecord的crc的数据数据的所需字段不同，请参见最上的注释。
func (d *LogRecord) crcData() ([]byte, error) {
	var err error
	bf := bfPool.Get().(*bytes.Buffer)
	defer func() {
		bf.Reset()
		bfPool.Put(bf)
	}()
	switch d.typ {
	case NormalRecord:
		bf.Grow(typeSz + tmStampSz + kszSz + vszSz + len(d.key) + len(d.value))
		// NOTE: binary.Write会使用反射有一定的开销，也是直接使用PutXXX需要的是[]byte而不是io.Writer
		if err = binary.Write(bf, defaultEndianness, d.typ); err != nil {
			return nil, err
		}
		if err = binary.Write(bf, defaultEndianness, d.tmStamp); err != nil {
			return nil, err
		}
		if err = binary.Write(bf, defaultEndianness, d.ksz); err != nil {
			return nil, err
		}
		if err = binary.Write(bf, defaultEndianness, d.valueSz); err != nil {
			return nil, err
		}
		if _, err = bf.Write(d.key); err != nil {
			return nil, err
		}
		if _, err = bf.Write(d.value); err != nil {
			return nil, err
		}
	case DeleteRecord:
		bf.Grow(typeSz + tmStampSz + kszSz + len(d.key))
		if err = binary.Write(bf, defaultEndianness, d.typ); err != nil {
			return nil, err
		}
		if err = binary.Write(bf, defaultEndianness, d.tmStamp); err != nil {
			return nil, err
		}
		if err = binary.Write(bf, defaultEndianness, d.ksz); err != nil {
			return nil, err
		}
		if _, err = bf.Write(d.key); err != nil {
			return nil, err
		}
	}

	return bf.Bytes(), nil
}

func (d *LogRecord) calCrc(crcInput []byte) {
	d.crc = crc32.ChecksumIEEE(crcInput)
}

func (d *LogRecord) Serialize() ([]byte, error) {
	bf := bfPool.Get().(*bytes.Buffer)
	defer func() {
		bf.Reset()
		bfPool.Put(bf)
	}()
	// 先计算crc
	crcData, err := d.crcData()
	if err != nil {
		return nil, err
	}
	//
	d.calCrc(crcData)
	bf.Grow(crcSz + len(crcData))
	if err := binary.Write(bf, defaultEndianness, d.crc); err != nil {
		return nil, err
	}
	if _, err := bf.Write(crcData); err != nil {
		return nil, err
	}
	return bf.Bytes(), nil
}

func (d *LogRecord) Deserialize(b []byte) (DataSerializer, error) {
	res := new(LogRecord)
	res.crc = defaultEndianness.Uint32(b[:crcSz])
	res.typ = LogRecordType(b[crcSz : crcSz+typeSz][0])

	switch res.typ {
	case NormalRecord:
		res.tmStamp = defaultEndianness.Uint64(b[crcSz+typeSz : crcSz+typeSz+tmStampSz])
		res.ksz = defaultEndianness.Uint64(b[crcSz+typeSz+tmStampSz : crcSz+typeSz+tmStampSz+kszSz])
		res.valueSz = defaultEndianness.Uint64(b[crcSz+typeSz+tmStampSz+kszSz : crcSz+typeSz+tmStampSz+kszSz+vszSz])
		res.key = b[crcSz+typeSz+tmStampSz+kszSz+vszSz : crcSz+typeSz+tmStampSz+kszSz+vszSz+res.ksz]
		res.value = b[crcSz+typeSz+tmStampSz+kszSz+vszSz+res.ksz : crcSz+typeSz+tmStampSz+kszSz+vszSz+res.ksz+res.valueSz]
	case DeleteRecord:
		res.tmStamp = defaultEndianness.Uint64(b[crcSz+typeSz : crcSz+typeSz+tmStampSz])
		res.ksz = defaultEndianness.Uint64(b[crcSz+typeSz+tmStampSz : crcSz+typeSz+tmStampSz+kszSz])
		res.key = b[crcSz+typeSz+tmStampSz+kszSz : crcSz+typeSz+tmStampSz+kszSz+res.ksz]
	}

	// 校验crc
	crcData, err := res.crcData()
	if err != nil {
		return nil, err
	}
	beforeCrc := res.crc
	res.calCrc(crcData)
	afterCrc := res.crc
	if beforeCrc != afterCrc {
		return nil, ErrCrcCheckFailed
	}
	return res, nil
}
