package file_manage

import (
	"fmt"
	"os"
	"path/filepath"
)

// FileManager 文件管理器
type FileManager interface {
	ReadAt(offset int64, size int64) ([]byte, error)
	Seek(offset int64, whence int) (int64, error)
	Write(data []byte) (int64, error)
	Close() error
	GetFileID() int64
	Size() (int64, error)
	Sync() error
	GetOffset() int64
	SetOffset(offset int64)
}

// NewFileManager 创建一个新的文件管理器
func NewFileManager(filePath string, fileID int64) (FileManager, error) {
	// 确保父目录存在
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
	}

	// 创建 FileIO 实例
	fileIO, err := NewFileIO(filePath, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to create file IO: %v", err)
	}

	return fileIO, nil
}
