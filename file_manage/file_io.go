package file_manage

import (
	"fmt"
	"os"
)

// FileIO 文件IO
type FileIO struct {
	File    *os.File // 文件
	DirPath string   // 目录路径
	FileID  int64    // 文件ID
	Offset  int64    // 偏移量
	// mu      sync.RWMutex // 添加互斥锁保护并发访问
}

func (f *FileIO) Seek(offset int64, whence int) (int64, error) {
	// f.mu.Lock()
	// defer f.mu.Unlock()
	return f.File.Seek(offset, whence)
}
func (f *FileIO) Write(data []byte) (int64, error) {
	// f.mu.Lock()
	// defer f.mu.Unlock()

	n, err := f.File.Write(data)
	if err != nil {
		return 0, fmt.Errorf("write error: %v", err)
	}
	// 更新偏移量
	f.Offset += int64(n)
	return int64(n), nil
}
func (f *FileIO) ReadAt(offset int64, size int64) ([]byte, error) {
	// f.mu.RLock()
	// defer f.mu.RUnlock()

	buf := make([]byte, size)
	_, err := f.File.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func NewFileIO(filePath string, fileID int64) (*FileIO, error) {
	// 打开文件，使用正确的打开模式
	fp, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	// 获取文件当前大小作为初始偏移量
	info, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	return &FileIO{
		File:    fp,
		DirPath: filePath,
		FileID:  fileID,
		Offset:  info.Size(),
	}, nil
}

func (f *FileIO) GetFileID() int64 {
	// f.mu.RLock()
	// defer f.mu.RUnlock()
	return f.FileID
}
func (f *FileIO) Size() (int64, error) {
	// f.mu.RLock()
	// defer f.mu.RUnlock()

	info, err := f.File.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
func (f *FileIO) Sync() error {
	// f.mu.Lock()
	// defer f.mu.Unlock()
	return f.File.Sync()
}
func (f *FileIO) Close() error {
	// f.mu.Lock()
	// defer f.mu.Unlock()
	return f.File.Close()
}
func (f *FileIO) GetOffset() int64 {
	// f.mu.RLock()
	// defer f.mu.RUnlock()
	return f.Offset
}
func (f *FileIO) SetOffset(offset int64) {
	// f.mu.Lock()
	// defer f.mu.Unlock()
	f.Offset = offset
}
