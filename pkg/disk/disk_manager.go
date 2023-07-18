package disk

import (
	"path"

	"bitcask-go/pkg/key_dir"
)

type Manager struct {
	persistent PersistentStorage // 持久化存储
	name       string
	suffix     uint64
}

func NewManager(p string, suffix uint64) (ActiveData, error) {
	var err error
	res := new(Manager)
	res.name = path.Base(p)
	res.suffix = suffix
	res.persistent, err = NewFilePersistentImpl(p, suffix)
	return res, err
}

func (m *Manager) Write(key, value []byte) (mv *key_dir.MemValue, err error) {
	item, err := NewNormalLogRecord(key, value)
	if err != nil {
		return
	}
	bs, err := item.Serialize()
	if err != nil {
		return
	}
	// TODO 如果超过了数据文件大小，应该在哪里判断会合适一点
	// 这里更合适一点，因为接口一致性更好，否则就需要在外面构造LogRecord，这明显不合适
	// 如果太大了,返回一个指定的Error告知。
	offset, wn, err := m.persistent.WriteToDisk(bs)
	if err != nil {
		return
	}
	mv = &key_dir.MemValue{
		FileID:   m.ID(),
		ValueSz:  uint64(wn),
		ValuePos: uint64(offset),
		TsTamp:   int64(item.tmStamp),
	}
	return
}

func (m *Manager) Read(mv *key_dir.MemValue) (value []byte, err error) {
	bs := make([]byte, mv.ValueSz)
	_, err = m.persistent.ReadFromDisk(bs, mv.ValuePos)
	if err != nil {
		return
	}
	item, err := new(LogRecord).Deserialize(bs)
	if err != nil {
		return
	}
	value = item.(*LogRecord).value
	return
}

func (m *Manager) ID() uint64 {
	return m.suffix
}

func (m *Manager) Close() error {
	return m.persistent.Close()
}

func (m *Manager) Delete() error {
	return m.persistent.Delete()
}
