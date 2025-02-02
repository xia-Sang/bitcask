package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/xia-Sang/bitcask/record"
)

func TestWAL(t *testing.T) {
	// 创建临时目录用于测试
	dir, err := os.MkdirTemp("", "wal-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	t.Run("Basic Operations", func(t *testing.T) {
		walFile := filepath.Join(dir, "1.wal")
		wal, err := NewWAL(walFile, 1)
		if err != nil {
			t.Fatalf("NewWAL failed: %v", err)
		}
		defer wal.Close()

		// 测试写入正常记录
		key := []byte("test_key")
		value := []byte("test_value")
		pos, err := wal.Append(key, value, record.RecordTypeNormal)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		// 验证返回的位置信息
		if pos.FileID != 1 {
			t.Errorf("Wrong FileID, got %d, want 1", pos.FileID)
		}
		if pos.Size <= 0 {
			t.Errorf("Invalid Size: %d", pos.Size)
		}
		if pos.Offset != 0 {
			t.Errorf("Wrong initial offset, got %d, want 0", pos.Offset)
		}
	})

	t.Run("Multiple Records", func(t *testing.T) {
		walFile := filepath.Join(dir, "2.wal")
		wal, err := NewWAL(walFile, 2)
		if err != nil {
			t.Fatalf("NewWAL failed: %v", err)
		}
		defer wal.Close()

		// 写入多条记录
		records := []struct {
			key   []byte
			value []byte
			typ   record.RecordType
		}{
			{[]byte("key1"), []byte("value1"), record.RecordTypeNormal},
			{[]byte("key2"), []byte("value2"), record.RecordTypeDeleted},
			{[]byte("key3"), []byte("value3"), record.RecordTypeCheckpoint},
		}

		var lastOffset int64 = 0
		for i, r := range records {
			pos, err := wal.Append(r.key, r.value, r.typ)
			if err != nil {
				t.Errorf("Append failed for key %s: %v", r.key, err)
				continue
			}

			// 添加调试信息
			t.Logf("Record %d: Offset=%d, Size=%d, LastOffset=%d",
				i, pos.Offset, pos.Size, lastOffset)

			// 验证偏移量是递增的
			if pos.Offset != lastOffset {
				t.Errorf("Wrong offset: got %d, want %d", pos.Offset, lastOffset)
			}

			// 更新下一条记录的期望偏移量
			lastOffset += pos.Size
		}

		// 验证最终文件大小
		fileInfo, err := os.Stat(walFile)
		if err != nil {
			t.Fatalf("Failed to stat WAL file: %v", err)
		}
		if fileInfo.Size() != lastOffset {
			t.Errorf("File size mismatch: got %d, want %d", fileInfo.Size(), lastOffset)
		}
	})

	t.Run("File Persistence", func(t *testing.T) {
		walFile := filepath.Join(dir, "3.wal")
		wal, err := NewWAL(walFile, 3)
		if err != nil {
			t.Fatalf("NewWAL failed: %v", err)
		}

		// 写入数据
		key := []byte("persist_key")
		value := []byte("persist_value")
		pos, err := wal.Append(key, value, record.RecordTypeNormal)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}

		// 同步并关闭
		if err := wal.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// 验证文件大小
		info, err := os.Stat(walFile)
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}
		if info.Size() != pos.Size {
			t.Errorf("File size mismatch: got %d, want %d", info.Size(), pos.Size)
		}

		// 重新打开文件验证内容持久化
		wal2, err := NewWAL(walFile, 3)
		if err != nil {
			t.Fatalf("Reopen WAL failed: %v", err)
		}
		defer wal2.Close()

		// 验证偏移量是否正确恢复
		if offset := wal2.GetOffset(); offset != pos.Size {
			t.Errorf("Offset not restored correctly: got %d, want %d", offset, pos.Size)
		}
	})

	t.Run("Edge Cases", func(t *testing.T) {
		walFile := filepath.Join(dir, "4.wal")
		wal, err := NewWAL(walFile, 4)
		if err != nil {
			t.Fatalf("NewWAL failed: %v", err)
		}
		defer wal.Close()

		// 测试空键
		_, err = wal.Append([]byte{}, []byte("value"), record.RecordTypeNormal)
		if err != nil {
			t.Logf("Got expected error for empty key: %v", err)
		}

		// 测试空值
		_, err = wal.Append([]byte("key"), []byte{}, record.RecordTypeNormal)
		if err != nil {
			t.Logf("Got expected error for empty value: %v", err)
		}

		// 测试大数据
		largeKey := bytes.Repeat([]byte("k"), 1000)
		largeValue := bytes.Repeat([]byte("v"), 1000)
		pos, err := wal.Append(largeKey, largeValue, record.RecordTypeNormal)
		if err != nil {
			t.Errorf("Failed to append large record: %v", err)
		} else {
			t.Logf("Successfully wrote large record: size=%d", pos.Size)
		}
	})

	t.Run("Invalid File Path", func(t *testing.T) {
		// 使用无效的文件路径
		invalidPath := filepath.Join("nonexistent", "invalid.wal")
		_, err := NewWAL(invalidPath, 1)
		if err == nil {
			t.Error("Expected error for invalid file path")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	})
}
