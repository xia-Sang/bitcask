package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/xia-Sang/bitcask/file_manage"
	"github.com/xia-Sang/bitcask/index"
	"github.com/xia-Sang/bitcask/record"
)

type WAL struct {
	fileIO file_manage.FileManager
}

func NewWAL(dirPath string, fileID int64) (*WAL, error) {
	fileIO, err := file_manage.NewFileManager(dirPath, fileID)
	if err != nil {
		return nil, err
	}
	return &WAL{
		fileIO: fileIO,
	}, nil
}
func (w *WAL) ReadAt(offset int64, length int64) ([]byte, error) {
	return w.fileIO.ReadAt(offset, length)
}
func (w *WAL) LoadWal(memIndex index.Index) error {
	// 读取WAL所有数据
	offset := int64(0)
	for {
		var headerLength int64 = 1 + 4 + 4
		headerBytes, err := w.ReadAt(offset, headerLength)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read at error: %v", err)
		}
		recordType := record.RecordType(headerBytes[0])
		keyLength := binary.BigEndian.Uint32(headerBytes[1:5])
		valueLength := binary.BigEndian.Uint32(headerBytes[5:9])

		var reminderDataLength int64 = int64(keyLength) + int64(valueLength) + 4
		offset += headerLength
		reminderData, err := w.ReadAt(offset, reminderDataLength)
		if err != nil {
			return fmt.Errorf("read at error: %v", err)
		}
		key := reminderData[:keyLength]
		_ = reminderData[keyLength : keyLength+valueLength]
		crc := binary.BigEndian.Uint32(reminderData[keyLength+valueLength : keyLength+valueLength+4])
		data := append(headerBytes, reminderData...)
		expectCrc := crc32.ChecksumIEEE(data[:headerLength+reminderDataLength-4])
		if crc != expectCrc {
			return fmt.Errorf("crc check failed")
		}
		offset += reminderDataLength
		length := headerLength + reminderDataLength
		pos := &record.Pos{
			FileID: w.GetFileID(),
			Offset: offset - length,
			Size:   length,
		}
		if recordType == record.RecordTypeNormal {
			if err := memIndex.Put(key, pos); err != nil {
				return fmt.Errorf("put to memIndex error: %v", err)
			}
		} else if recordType == record.RecordTypeDeleted {
			if err := memIndex.Delete(key); err != nil {
				return fmt.Errorf("delete from memIndex error: %v", err)
			}
		}
	}
	w.SetOffset(offset)
	return nil
}
func (w *WAL) Append(key []byte, value []byte, typ record.RecordType) (*record.Pos, error) {
	// 创建记录头
	header := &record.Header{
		Key:        key,
		Value:      value,
		RecordType: typ,
	}

	// 序列化记录
	data := header.ToBytes()

	// 获取写入前的偏移量
	startOffset := w.fileIO.GetOffset()

	// 写入数据
	n, err := w.fileIO.Write(data)
	if err != nil {
		return nil, fmt.Errorf("write error: %v", err)
	}

	// 确保数据已写入磁盘
	if err := w.fileIO.Sync(); err != nil {
		return nil, fmt.Errorf("sync error: %v", err)
	}

	// 返回位置信息
	pos := &record.Pos{
		FileID: w.fileIO.GetFileID(),
		Offset: startOffset,
		Size:   n,
	}

	return pos, nil
}

func (w *WAL) Write(data []byte) (int64, error) {
	return w.fileIO.Write(data)
}

func (w *WAL) Sync() error {
	return w.fileIO.Sync()
}

func (w *WAL) Close() error {
	return w.fileIO.Close()
}

func (w *WAL) GetFileID() int64 {
	return w.fileIO.GetFileID()
}

func (w *WAL) GetOffset() int64 {
	return w.fileIO.GetOffset()
}

func (w *WAL) SetOffset(offset int64) {
	w.fileIO.SetOffset(offset)
}
