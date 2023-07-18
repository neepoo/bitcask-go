package disk

import (
	"os"
	"sync"
)

var (
	filePerm = os.FileMode(0600)
	fileFlag = os.O_CREATE | os.O_RDWR | os.O_APPEND // 读写追加
)

// FilePersistentImpl 持久化存储接口的本地文件
type FilePersistentImpl struct {
	file   *os.File
	offset int64
	suffix uint64
	sync.RWMutex
}

func (f *FilePersistentImpl) getOffset() int64 {
	f.RLock()
	res := f.offset
	f.RUnlock()
	return res
}

func (f *FilePersistentImpl) setOffset(offset int64) {
	f.Lock()
	f.offset = offset
	f.Unlock()
}

func NewFilePersistentImpl(path string, suffix uint64) (PersistentStorage, error) {
	var err error
	res := new(FilePersistentImpl)
	res.file, err = os.OpenFile(path, fileFlag, filePerm)
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
	offset = f.getOffset()
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
