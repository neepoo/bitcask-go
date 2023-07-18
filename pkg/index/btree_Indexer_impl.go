package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"
)

type Btree struct {
	tree *btree.BTree
	sync.RWMutex
}

func NewBtree() Indexer {
	return &Btree{tree: btree.New(32)}
}

type BTreeItem struct {
	Key []byte
	Val *ValueMetadata
}

func (B BTreeItem) Less(other btree.Item) bool {
	return bytes.Compare(B.Key, other.(BTreeItem).Key) < 0
}

func (b *Btree) Get(key []byte) (*ValueMetadata, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	b.RLock()
	defer b.RUnlock()
	item := b.tree.Get(BTreeItem{Key: key})
	if item == nil {
		return nil, nil
	}
	return item.(BTreeItem).Val, nil
}

func (b *Btree) Set(key []byte, value *ValueMetadata) error {
	if err := checkKey(key); err != nil {
		return err
	}
	b.Lock()
	b.tree.ReplaceOrInsert(BTreeItem{Key: key, Val: value})
	b.Unlock()
	return nil
}

func (b *Btree) Del(key []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	b.Lock()
	b.tree.Delete(BTreeItem{Key: key})
	b.Unlock()
	return nil
}
