package bitcask

import (
	"bytes"
	"testing"

	"github.com/xia-Sang/bitcask/utils"
)

func TestBitcask(t *testing.T) {
	conf := &Config{
		DirPath:     "./test",
		MaxFileSize: 512,
		IndexType:   "btree",
	}
	db, err := NewBitcask(conf)
	if err != nil {
		t.Fatalf("failed to create bitcask: %v", err)
	}
	ma := make(map[string][]byte)
	for i := 0; i < 120; i++ {
		key, value := utils.GenerateKey(i), utils.GenerateValue(10)
		db.Put(key, value)
		ma[string(key)] = value
	}
	// db.Show()
	db.Close()
	db, err = NewBitcask(conf)
	if err != nil {
		t.Fatalf("failed to create bitcask: %v", err)
	}
	for i := 0; i < 20; i++ {
		key := utils.GenerateKey(i)
		value, ok := db.Get(key)
		if !ok {
			t.Fatalf("failed to get key %s,value %s", string(key), string(value))
		}
		if !bytes.Equal(value, ma[string(key)]) {
			t.Fatalf("value mismatch for key %s", string(key))
		}
	}
	db.Close()
}
