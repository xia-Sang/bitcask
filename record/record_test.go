package record

import (
	"bytes"
	"testing"
)

func TestRecordSerialization(t *testing.T) {
	t.Run("Basic Serialization", func(t *testing.T) {
		header := &Header{
			Key:        []byte("test_key"),
			Value:      []byte("test_value"),
			RecordType: RecordTypeNormal,
		}

		// 测试序列化
		data := header.ToBytes()
		if data == nil {
			t.Fatal("ToBytes() returned nil")
		}

		// 测试反序列化
		result := FromBytes(data)
		if result == nil {
			t.Fatal("FromBytes() returned nil")
		}

		// 验证字段
		if !bytes.Equal(result.Key, header.Key) {
			t.Errorf("Key mismatch: got %s, want %s", result.Key, header.Key)
		}
		if !bytes.Equal(result.Value, header.Value) {
			t.Errorf("Value mismatch: got %s, want %s", result.Value, header.Value)
		}
		if result.RecordType != header.RecordType {
			t.Errorf("RecordType mismatch: got %d, want %d", result.RecordType, header.RecordType)
		}
	})

	t.Run("Different Record Types", func(t *testing.T) {
		testCases := []struct {
			name       string
			recordType RecordType
		}{
			{"Normal", RecordTypeNormal},
			{"Deleted", RecordTypeDeleted},
			{"Checkpoint", RecordTypeCheckpoint},
			{"TransactionBegin", RecordTypeTransactionBegin},
			{"TransactionCommit", RecordTypeTransactionCommit},
			{"TransactionRollback", RecordTypeTransactionRollback},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				header := &Header{
					Key:        []byte("key"),
					Value:      []byte("value"),
					RecordType: tc.recordType,
				}

				data := header.ToBytes()
				result := FromBytes(data)

				if result == nil {
					t.Fatal("FromBytes() returned nil")
				}
				if result.RecordType != tc.recordType {
					t.Errorf("RecordType mismatch: got %d, want %d", result.RecordType, tc.recordType)
				}
			})
		}
	})

	t.Run("Edge Cases", func(t *testing.T) {
		t.Run("Empty Key", func(t *testing.T) {
			header := &Header{
				Key:        []byte{},
				Value:      []byte("value"),
				RecordType: RecordTypeNormal,
			}

			data := header.ToBytes()
			result := FromBytes(data)

			if result == nil {
				t.Fatal("FromBytes() returned nil")
			}
			if len(result.Key) != 0 {
				t.Errorf("Expected empty key, got %v", result.Key)
			}
		})

		t.Run("Empty Value", func(t *testing.T) {
			header := &Header{
				Key:        []byte("key"),
				Value:      []byte{},
				RecordType: RecordTypeNormal,
			}

			data := header.ToBytes()
			result := FromBytes(data)

			if result == nil {
				t.Fatal("FromBytes() returned nil")
			}
			if len(result.Value) != 0 {
				t.Errorf("Expected empty value, got %v", result.Value)
			}
		})

		t.Run("Large Key and Value", func(t *testing.T) {
			largeKey := bytes.Repeat([]byte("k"), 1000)
			largeValue := bytes.Repeat([]byte("v"), 1000)
			header := &Header{
				Key:        largeKey,
				Value:      largeValue,
				RecordType: RecordTypeNormal,
			}

			data := header.ToBytes()
			result := FromBytes(data)

			if result == nil {
				t.Fatal("FromBytes() returned nil")
			}
			if !bytes.Equal(result.Key, largeKey) {
				t.Error("Large key mismatch")
			}
			if !bytes.Equal(result.Value, largeValue) {
				t.Error("Large value mismatch")
			}
		})
	})

	t.Run("Data Corruption", func(t *testing.T) {
		t.Run("Invalid Data Length", func(t *testing.T) {
			result := FromBytes([]byte{1, 2, 3}) // 太短的数据
			if result != nil {
				t.Error("Expected nil for invalid data length")
			}
		})

		t.Run("Invalid CRC", func(t *testing.T) {
			header := &Header{
				Key:        []byte("key"),
				Value:      []byte("value"),
				RecordType: RecordTypeNormal,
			}

			data := header.ToBytes()
			// 破坏数据的CRC
			data[len(data)-1] ^= 1

			result := FromBytes(data)
			if result != nil {
				t.Error("Expected nil for corrupted data")
			}
		})

		t.Run("Invalid Record Type", func(t *testing.T) {
			header := &Header{
				Key:        []byte("key"),
				Value:      []byte("value"),
				RecordType: RecordType(255), // 无效的记录类型
			}

			data := header.ToBytes()
			result := FromBytes(data)
			if result == nil {
				t.Fatal("FromBytes() returned nil")
			}
			if result.RecordType != RecordType(255) {
				t.Errorf("RecordType mismatch: got %d, want %d", result.RecordType, RecordType(255))
			}
		})
	})

	t.Run("String Representation", func(t *testing.T) {
		header := &Header{
			Key:        []byte("test_key"),
			Value:      []byte("test_value"),
			RecordType: RecordTypeNormal,
		}

		str := header.String()
		expected := "Key: test_key, Value: test_value, RecordType: 0"
		if str != expected {
			t.Errorf("String() returned %q, want %q", str, expected)
		}
	})
}
