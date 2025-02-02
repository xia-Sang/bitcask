package index

import "github.com/xia-Sang/bitcask/record"

type Index interface {
	Get(key []byte) (pos *record.Pos, err error)
	Put(key []byte, pos *record.Pos) error
	Delete(key []byte) error
	Len() int
	Iterator() IndexIter
}

type IndexIter interface {
	Prev()
	Next()
	Seek(key []byte)
	Valid() bool
	Key() []byte
	Value() *record.Pos
	Close()
}

func NewIndex(typ string) Index {
	switch typ {
	case "btree":
		return NewBTreeIndex(12)
	}
	return nil
}
