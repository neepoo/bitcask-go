package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"time"
)

const (
	crcSz     = 4 // uint32
	tmStampSz = 8 // uint64
	kszSz     = 8 // uint64
	vszSz     = 8 // uint64
)

// defaultEndianness 默认的字节序，小端序
var defaultEndianness = binary.LittleEndian
var ErrCrcCheckFailed = errors.New("crc check failed")
var bfPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// Item 表示bitcask磁盘文件中的一个条目
// 完全按照bitcask的格式存储
type Item struct {
	// crc校验码
	crc uint32
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

func NewItem(k, v []byte) (*Item, error) {
	res := new(Item)
	res.key = k
	res.value = v
	res.tmStamp = uint64(time.Now().Unix())
	res.ksz = uint64(len(k))
	res.valueSz = uint64(len(v))
	crcInput, err := res.CrcData()
	if err != nil {
		return nil, err
	}
	res.CalCrc(crcInput)
	return res, nil
}

// CrcData 得到除了crc字段之外的所有字段
func (d *Item) CrcData() ([]byte, error) {
	var err error
	bf := bfPool.Get().(*bytes.Buffer)
	defer func() {
		bf.Reset()
		bfPool.Put(bf)
	}()
	bf.Grow(tmStampSz + kszSz + vszSz + len(d.key) + len(d.value))
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
	return bf.Bytes(), nil
}

func (d *Item) CalCrc(crcInput []byte) {
	d.crc = crc32.ChecksumIEEE(crcInput)
}

func (d *Item) Serialize() ([]byte, error) {
	bf := bfPool.Get().(*bytes.Buffer)
	defer func() {
		bf.Reset()
		bfPool.Put(bf)
	}()
	// 先计算crc
	crcData, err := d.CrcData()
	if err != nil {
		return nil, err
	}
	//
	d.CalCrc(crcData)
	bf.Grow(crcSz + len(crcData))
	if err := binary.Write(bf, defaultEndianness, d.crc); err != nil {
		return nil, err
	}
	if _, err := bf.Write(crcData); err != nil {
		return nil, err
	}
	return bf.Bytes(), nil
}

func (d *Item) Deserialize(b []byte) (DataSerializer, error) {
	res := new(Item)
	res.crc = defaultEndianness.Uint32(b[:crcSz])
	res.tmStamp = defaultEndianness.Uint64(b[crcSz : crcSz+tmStampSz])
	res.ksz = defaultEndianness.Uint64(b[crcSz+tmStampSz : crcSz+tmStampSz+kszSz])
	res.valueSz = defaultEndianness.Uint64(b[crcSz+tmStampSz+kszSz : crcSz+tmStampSz+kszSz+vszSz])
	res.key = b[crcSz+tmStampSz+kszSz+vszSz : crcSz+tmStampSz+kszSz+vszSz+res.ksz]
	res.value = b[crcSz+tmStampSz+kszSz+vszSz+res.ksz : crcSz+tmStampSz+kszSz+vszSz+res.ksz+res.valueSz]

	// 校验crc
	crcData, err := res.CrcData()
	if err != nil {
		return nil, err
	}
	beforeCrc := res.crc
	res.CalCrc(crcData)
	afterCrc := res.crc
	if beforeCrc != afterCrc {
		return nil, ErrCrcCheckFailed
	}
	return res, nil
}
