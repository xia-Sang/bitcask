package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type RecordType byte

const (
	RecordTypeNormal              RecordType = iota // 正常记录
	RecordTypeDeleted                               // 已删除记录
	RecordTypeCheckpoint                            // 检查点记录
	RecordTypeTransactionBegin                      // 事务开始
	RecordTypeTransactionCommit                     // 事务提交
	RecordTypeTransactionRollback                   // 事务回滚
)

// Header 记录头
type Header struct {
	Key        []byte     // 键
	Value      []byte     // 值
	RecordType RecordType // 记录类型
}

// String 实现 Stringer 接口
func (h *Header) String() string {
	return fmt.Sprintf("Key: %s, Value: %s, RecordType: %d", h.Key, h.Value, h.RecordType)
}

// ToBytes 实现转为bytes
// recordtype keysize valuesize key value crc32
func (h *Header) ToBytes() []byte {
	// 1.计算总长度：recordType(1) + keySize(4) + valueSize(4) + key + value + crc32(4)
	headerLength := 1 + 4 + 4 + len(h.Key) + len(h.Value) + 4
	buf := make([]byte, headerLength)

	// 2.写入数据
	buf[0] = byte(h.RecordType)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(h.Key)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(h.Value)))
	copy(buf[9:9+len(h.Key)], h.Key)
	copy(buf[9+len(h.Key):9+len(h.Key)+len(h.Value)], h.Value)

	// 3.计算并写入crc32（不包含crc32字段本身）
	crc := crc32.ChecksumIEEE(buf[:headerLength-4])
	binary.BigEndian.PutUint32(buf[headerLength-4:], crc)

	return buf
}

func FromBytes(data []byte) *Header {
	header := &Header{}
	if len(data) < 9 { // 至少需要recordType(1) + keySize(4) + valueSize(4)
		return nil
	}

	header.RecordType = RecordType(data[0])
	keySize := binary.BigEndian.Uint32(data[1:5])
	valueSize := binary.BigEndian.Uint32(data[5:9])

	// 验证数据长度是否足够
	expectedLen := 9 + keySize + valueSize + 4 // header(9) + key + value + crc32(4)
	if uint32(len(data)) < expectedLen {
		return nil
	}

	// 读取key和value
	header.Key = make([]byte, keySize)
	header.Value = make([]byte, valueSize)
	copy(header.Key, data[9:9+keySize])
	copy(header.Value, data[9+keySize:9+keySize+valueSize])

	// 验证CRC32
	crc := binary.BigEndian.Uint32(data[9+keySize+valueSize:])
	if crc != crc32.ChecksumIEEE(data[:9+keySize+valueSize]) {
		return nil
	}

	return header
}
