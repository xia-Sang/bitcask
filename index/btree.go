package index

import (
	"bytes"
	"errors"
	"sync"

	"github.com/google/btree"
	"github.com/xia-Sang/bitcask/record"
)

// BTreeIndex 实现 Index 接口
type BTreeIndex struct {
	mu    sync.RWMutex
	items *btree.BTree
}

// indexItem 实现 btree.Item 接口
type indexItem struct {
	key []byte
	pos *record.Pos
}

// Less 实现 btree.Item 接口的比较方法
func (i *indexItem) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*indexItem).key) < 0
}

// NewBTreeIndex 创建一个新的索引实例
func NewBTreeIndex(degree int) *BTreeIndex {
	return &BTreeIndex{
		items: btree.New(degree),
	}
}

// Get 获取键对应的位置信息
func (idx *BTreeIndex) Get(key []byte) (*record.Pos, error) {
	if len(key) == 0 {
		return nil, errors.New("key is empty")
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	item := idx.items.Get(&indexItem{key: key})
	if item == nil {
		return nil, errors.New("key not found")
	}
	return item.(*indexItem).pos, nil
}

// Put 将键值对的位置信息存入索引
func (idx *BTreeIndex) Put(key []byte, pos *record.Pos) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}
	if pos == nil {
		return errors.New("pos is nil")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.items.ReplaceOrInsert(&indexItem{
		key: key,
		pos: pos,
	})
	return nil
}

// Delete 从索引中删除键
func (idx *BTreeIndex) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	item := idx.items.Delete(&indexItem{key: key})
	if item == nil {
		return errors.New("key not found")
	}
	return nil
}

// Len 返回索引中的键值对数量
func (idx *BTreeIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.items.Len()
}

// Iterator 返回一个迭代器
func (idx *BTreeIndex) Iterator() IndexIter {
	return NewBTreeIterator(idx)
}

// BTreeIterator 实现 IndexIter 接口
type BTreeIterator struct {
	idx       *BTreeIndex
	currItem  *indexItem
	items     []*indexItem
	currIndex int
}

func NewBTreeIterator(idx *BTreeIndex) *BTreeIterator {
	iter := &BTreeIterator{
		idx:       idx,
		items:     make([]*indexItem, 0),
		currIndex: -1,
	}

	// 收集所有项
	idx.mu.RLock()
	idx.items.Ascend(func(i btree.Item) bool {
		iter.items = append(iter.items, i.(*indexItem))
		return true
	})
	idx.mu.RUnlock()

	if len(iter.items) > 0 {
		iter.currIndex = 0
		iter.currItem = iter.items[0]
	}

	return iter
}

func (iter *BTreeIterator) Prev() {
	if iter.currIndex > 0 {
		iter.currIndex--
		iter.currItem = iter.items[iter.currIndex]
	} else {
		iter.currItem = nil
	}
}

func (iter *BTreeIterator) Next() {
	if iter.currIndex < len(iter.items)-1 {
		iter.currIndex++
		iter.currItem = iter.items[iter.currIndex]
	} else {
		iter.currItem = nil
	}
}

func (iter *BTreeIterator) Seek(key []byte) {
	for i, item := range iter.items {
		if bytes.Compare(item.key, key) >= 0 {
			iter.currIndex = i
			iter.currItem = item
			return
		}
	}
	iter.currItem = nil
}

func (iter *BTreeIterator) Valid() bool {
	return iter.currItem != nil
}

func (iter *BTreeIterator) Key() []byte {
	if iter.currItem == nil {
		return nil
	}
	return iter.currItem.key
}

func (iter *BTreeIterator) Value() *record.Pos {
	if iter.currItem == nil {
		return nil
	}
	return iter.currItem.pos
}

func (iter *BTreeIterator) Close() {
	iter.currItem = nil
	iter.items = nil
}
