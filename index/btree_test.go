package index

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/xia-Sang/bitcask/record"
)

func TestBTree(t *testing.T) {
	// 创建索引实例
	idx := NewIndex("btree")

	t.Run("Basic Operations", func(t *testing.T) {
		// 测试Put和Get
		key := []byte("test_key")
		pos := &record.Pos{FileID: 1, Offset: 100, Size: 200}

		err := idx.Put(key, pos)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}

		got, err := idx.Get(key)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if got.FileID != pos.FileID || got.Offset != pos.Offset || got.Size != pos.Size {
			t.Errorf("Get returned wrong position, got %v, want %v", got, pos)
		}

		// 测试Delete
		err = idx.Delete(key)
		if err != nil {
			t.Errorf("Delete failed: %v", err)
		}

		// 确认删除成功
		_, err = idx.Get(key)
		if err == nil {
			t.Error("Get should fail after Delete")
		}
	})

	t.Run("Multiple Items", func(t *testing.T) {
		// 插入多个键值对
		items := map[string]*record.Pos{
			"key1": {FileID: 1, Offset: 100, Size: 200},
			"key2": {FileID: 1, Offset: 300, Size: 200},
			"key3": {FileID: 1, Offset: 500, Size: 200},
		}

		for k, v := range items {
			err := idx.Put([]byte(k), v)
			if err != nil {
				t.Errorf("Put failed for key %s: %v", k, err)
			}
		}

		// 验证长度
		if idx.Len() != len(items) {
			t.Errorf("Wrong length, got %d, want %d", idx.Len(), len(items))
		}

		// 验证所有项都能正确获取
		for k, want := range items {
			got, err := idx.Get([]byte(k))
			if err != nil {
				t.Errorf("Get failed for key %s: %v", k, err)
			}
			if got.FileID != want.FileID || got.Offset != want.Offset || got.Size != want.Size {
				t.Errorf("Wrong position for key %s, got %v, want %v", k, got, want)
			}
		}
	})

	t.Run("Iterator", func(t *testing.T) {
		// 清空之前的数据
		idx = NewIndex("btree")

		// 插入有序的键值对
		items := []struct {
			key string
			pos *record.Pos
		}{
			{"a", &record.Pos{FileID: 1, Offset: 100, Size: 100}},
			{"b", &record.Pos{FileID: 1, Offset: 200, Size: 100}},
			{"c", &record.Pos{FileID: 1, Offset: 300, Size: 100}},
		}

		for _, item := range items {
			err := idx.Put([]byte(item.key), item.pos)
			if err != nil {
				t.Errorf("Put failed for key %s: %v", item.key, err)
			}
		}

		// 测试正向遍历
		iter := idx.Iterator()
		i := 0
		for iter.Valid() {
			if !bytes.Equal(iter.Key(), []byte(items[i].key)) {
				t.Errorf("Wrong key at position %d, got %s, want %s",
					i, string(iter.Key()), items[i].key)
			}
			pos := iter.Value()
			if pos.FileID != items[i].pos.FileID ||
				pos.Offset != items[i].pos.Offset ||
				pos.Size != items[i].pos.Size {
				t.Errorf("Wrong position at key %s, got %v, want %v",
					items[i].key, pos, items[i].pos)
			}
			i++
			iter.Next()
		}
		iter.Close()

		// 测试Seek
		iter = idx.Iterator()
		iter.Seek([]byte("b"))
		if !bytes.Equal(iter.Key(), []byte("b")) {
			t.Errorf("Seek to 'b' failed, got key %s", string(iter.Key()))
		}
		iter.Close()
	})

	t.Run("Edge Cases", func(t *testing.T) {
		// 测试空键
		_, err := idx.Get([]byte{})
		if err == nil {
			t.Error("Get with empty key should fail")
		}

		// 测试nil键
		_, err = idx.Get(nil)
		if err == nil {
			t.Error("Get with nil key should fail")
		}

		// 测试删除不存在的键
		err = idx.Delete([]byte("nonexistent"))
		if err == nil {
			t.Error("Delete nonexistent key should fail")
		}

		// 测试重复Put相同的键
		key := []byte("duplicate")
		pos1 := &record.Pos{FileID: 1, Offset: 100, Size: 100}
		pos2 := &record.Pos{FileID: 1, Offset: 200, Size: 100}

		err = idx.Put(key, pos1)
		if err != nil {
			t.Errorf("First Put failed: %v", err)
		}

		err = idx.Put(key, pos2)
		if err != nil {
			t.Errorf("Second Put failed: %v", err)
		}

		// 验证最新的值
		got, err := idx.Get(key)
		if err != nil {
			t.Errorf("Get failed after duplicate Put: %v", err)
		}
		if got.Offset != pos2.Offset {
			t.Errorf("Wrong position after duplicate Put, got %v, want %v", got, pos2)
		}
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		// 创建新的索引实例
		idx := NewIndex("btree")
		done := make(chan bool)

		// 并发写入
		go func() {
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key%d", i))
				pos := &record.Pos{FileID: 1, Offset: int64(i * 100), Size: 100}
				err := idx.Put(key, pos)
				if err != nil {
					t.Errorf("Concurrent Put failed: %v", err)
				}
			}
			done <- true
		}()

		// 并发读取
		go func() {
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key%d", i))
				_, _ = idx.Get(key)
			}
			done <- true
		}()

		// 等待两个goroutine完成
		<-done
		<-done

		// 验证最终长度
		if idx.Len() != 100 {
			t.Errorf("Wrong length after concurrent operations, got %d, want %d",
				idx.Len(), 100)
		}
	})
}
