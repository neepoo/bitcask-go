package bitcast_go

import (
	"errors"
	"sync/atomic"

	"bitcask-go/pkg/disk"
	"bitcask-go/pkg/index"
)

type DB struct {
	Opts       *Options
	maxFileID  atomic.Uint64
	index      index.Indexer
	activeFile disk.DataFile
	oldFiles   map[uint64]disk.DataFile
}

func NewDb(opts *Options) *DB {
	db := new(DB)
	db.Opts = opts
	db.index = index.NewBtree()
	return db
}

// Put - put key-value to db
func (d *DB) Put(key, value []byte) (err error) {
	if d.activeFile == nil {
		d.activeFile, err = disk.NewManager(d.Opts.Dir, d.maxFileID.Add(1), true, d.Opts.MaxSize)
		if err != nil {
			return
		}
	}

	vMeta, err := d.activeFile.Write(key, value, false)
	if errors.Is(err, disk.ErrFileTooSmall) {
		// active file to old file
		oldFile, err := d.activeFile.ToOlderFile()
		if err != nil {
			return err
		}
		d.oldFiles[oldFile.ID()] = oldFile
		d.activeFile, err = disk.NewManager(d.Opts.Dir, d.maxFileID.Add(1), true, d.Opts.MaxSize)
		if err != nil {
			return err
		}
		vMeta, err = d.activeFile.Write(key, value, true)
		if err != nil {
			return err
		}
	}

	return d.index.Set(key, vMeta)

}

// Get - get value from db
func (d *DB) Get(key []byte) (value []byte, err error) {
	vMeta, err := d.index.Get(key)
	if err != nil {
		return
	}
	if vMeta == nil && err == nil {
		return nil, nil
	}
	return d.activeFile.Read(vMeta)
}

func (d *DB) Close() error {
	if err := d.activeFile.Close(); err != nil {
		return err
	}

	// 返回遇到的第一个错误
	var err error
	for _, v := range d.oldFiles {
		newErr := v.Close()
		if err == nil && newErr != nil {
			err = newErr
		}
	}
	return err
}
