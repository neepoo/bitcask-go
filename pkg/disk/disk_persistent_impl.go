package disk

import (
	"os"
	"path"
	"sync"
)

var (
	filePerm       = os.FileMode(0600)
	activeFileFlag = os.O_CREATE | os.O_RDWR | os.O_APPEND // active file 读写追加
	olderFileFlag  = os.O_RDONLY                           // older file 读
)

// FilePersistentImpl 持久化存储接口的本地文件
type FilePersistentImpl struct {
	file        *os.File
	writeOffset int64
	suffix      uint64
	sync.RWMutex
}

func (f *FilePersistentImpl) Offset() int64 {
	f.RLock()
	res := f.writeOffset
	f.RUnlock()
	return res
}

func (f *FilePersistentImpl) setOffset(offset int64) {
	f.Lock()
	f.writeOffset = offset
	f.Unlock()
}

func NewFilePersistentImpl(absPath string, suffix uint64, isActiveFile bool) (PersistentStorage, error) {
	var err error
	res := new(FilePersistentImpl)
	err = os.MkdirAll(path.Dir(absPath), os.FileMode(0755))
	if err != nil {
		return nil, err
	}
	if isActiveFile {
		res.file, err = os.OpenFile(absPath, activeFileFlag, filePerm)
	} else {
		res.file, err = os.OpenFile(absPath, olderFileFlag, filePerm)
	}
	if err != nil {
		return nil, err
	}
	res.suffix = suffix
	return res, nil
}

func (f *FilePersistentImpl) ReadFromDisk(bs []byte, offset uint64) (int, error) {
	// 这里还需要假定操作系统并发读取文件是安全的
	f.RLock()
	defer f.RUnlock()
	return f.file.ReadAt(bs, int64(offset))
}

func (f *FilePersistentImpl) WriteToDisk(bs []byte) (offset int64, wn int, err error) {
	f.Lock()
	wn, err = f.file.Write(bs)
	f.Unlock()
	offset = f.Offset()
	f.setOffset(offset + int64(wn))
	return
}

func (f *FilePersistentImpl) Sync() error {
	f.Lock()
	defer f.Lock()
	return f.file.Sync()
}

func (f *FilePersistentImpl) Close() error {
	f.Lock()
	defer f.Unlock()
	return f.file.Close()
}

func (f *FilePersistentImpl) Delete() error {
	f.Lock()
	defer f.Unlock()
	return os.Remove(f.file.Name())
}
