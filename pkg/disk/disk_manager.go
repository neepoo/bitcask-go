package disk

import (
	"bitcask-go/pkg/index"
	"errors"
	"fmt"
)

const dataFileExt = ".db"

var ErrFileTooSmall = errors.New("file too small")

type DataFileImpl struct {
	persistent PersistentStorage // 持久化存储
	maxSize    int64             // 当前文件的最大大小
	name       string
	suffix     uint64
}

func NewManager(dir string, suffix uint64, isActiveFile bool, maxSize int64) (DataFile, error) {
	var err error
	res := new(DataFileImpl)
	res.suffix = suffix
	res.name = fmt.Sprintf("%s/%06d%s", dir, suffix, dataFileExt)
	res.persistent, err = NewFilePersistentImpl(res.name, suffix, isActiveFile)
	if isActiveFile {
		// 只有activeFile才会有maxSize
		res.maxSize = maxSize
	}
	return res, err
}

// 判断将LogRecord持久化存储时是否会超过文件大小限制
// 除了常规情况还有就是就算新开一个文件也无法存储的情况，这种情况下就会暂时忽略文件大小限制，在新的文件中存储logRecord
func (m *DataFileImpl) checkExceedFileSizeLimit(record *LogRecord) error {
	// 常规情况
	if record.Size()+m.persistent.Offset() > m.maxSize {
		return ErrFileTooSmall
	}
	// 新开一个文件也无法存储的情况
	if record.Size() > m.maxSize {
		return ErrFileTooSmall
	}
	return nil
}

func (m *DataFileImpl) Write(key, value []byte, force bool) (wv *index.ValueMetadata, err error) {
	normalLogRecord, err := NewNormalLogRecord(key, value)
	if err != nil {
		return
	}
	if !force {
		if err = m.checkExceedFileSizeLimit(normalLogRecord); err != nil {
			return
		}
	}

	bs, err := normalLogRecord.Serialize()
	if err != nil {
		return
	}

	offset, wn, err := m.persistent.WriteToDisk(bs)
	if err != nil {
		return
	}
	wv = index.NewValueMetadata(m.ID(), uint64(wn), uint64(offset), int64(normalLogRecord.tmStamp))
	return
}

func (m *DataFileImpl) Del(key []byte, force bool) (dv *index.ValueMetadata, err error) {
	delLogRecord, err := NewDeleteLogRecord(key)
	if err != nil {
		return
	}
	if !force {
		if err = m.checkExceedFileSizeLimit(delLogRecord); err != nil {
			return
		}
	}

	bs, err := delLogRecord.Serialize()
	if err != nil {
		return
	}
	offset, wn, err := m.persistent.WriteToDisk(bs)
	if err != nil {
		return
	}
	dv = index.NewValueMetadata(m.ID(), uint64(wn), uint64(offset), int64(delLogRecord.tmStamp))
	return

}

func (m *DataFileImpl) Read(mv *index.ValueMetadata) (value []byte, err error) {
	bs := make([]byte, mv.ValueSz)
	_, err = m.persistent.ReadFromDisk(bs, mv.ValuePos)
	if err != nil {
		return
	}
	item, err := new(LogRecord).Deserialize(bs)
	if err != nil {
		return
	}
	value = item.Value()
	return
}

func (m *DataFileImpl) ID() uint64 {
	return m.suffix
}

func (m *DataFileImpl) Close() error {
	return m.persistent.Close()
}

func (m *DataFileImpl) Delete() error {
	return m.persistent.Delete()
}

func (m *DataFileImpl) ToOlderFile() (DataFile, error) {
	var err error
	if err = m.Close(); err != nil {
		return nil, err
	}

	newDataFile := m
	newDataFile.persistent, err = NewFilePersistentImpl(m.name, m.suffix, false)
	return newDataFile, err
}
