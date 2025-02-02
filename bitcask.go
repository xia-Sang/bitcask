package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/xia-Sang/bitcask/index"
	"github.com/xia-Sang/bitcask/record"
	"github.com/xia-Sang/bitcask/wal"
)

type Bitcask struct {
	config    *Config            // 配置
	olderWal  map[int64]*wal.WAL // 旧的wal
	activeWal *wal.WAL           // 当前的wal
	curIndex  index.Index        // 内存索引
	mu        sync.RWMutex       // 读写锁
	fileId    int64              // 当前文件id
	fileIds   []int64            // 所有文件id
}

func NewBitcask(config *Config) (*Bitcask, error) {
	// 确保主目录存在
	if err := os.MkdirAll(config.DirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// 确保 WAL 目录存在
	walDir := getWalDir(config.DirPath)
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	db := &Bitcask{
		config:   config,
		olderWal: make(map[int64]*wal.WAL),
		curIndex: index.NewIndex(config.IndexType),
		mu:       sync.RWMutex{},
		fileId:   0,
		fileIds:  make([]int64, 0),
	}

	// 初始化olderWal
	if err := db.load(); err != nil {
		return nil, fmt.Errorf("failed to load wal files: %v", err)
	}

	// 如果当前没有活跃的wal，则创建一个新的wal
	if db.activeWal == nil {
		walFile := filepath.Join(getWalDir(config.DirPath), getWalFileName(db.fileId))
		currWal, err := wal.NewWAL(walFile, db.fileId)
		if err != nil {
			return nil, fmt.Errorf("failed to create new wal file %s: %v", walFile, err)
		}
		db.activeWal = currWal
	}

	return db, nil
}

func (b *Bitcask) load() error {
	// 获取目录下所有WAL文件
	walDir := getWalDir(b.config.DirPath)
	files, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			// WAL目录不存在，这是正常的，创建一个新的WAL文件
			walFile := filepath.Join(walDir, getWalFileName(0))
			activeWal, err := wal.NewWAL(walFile, 0)
			if err != nil {
				return fmt.Errorf("failed to create initial WAL file: %v", err)
			}
			b.activeWal = activeWal
			return nil
		}
		return fmt.Errorf("failed to read wal directory: %v", err)
	}

	// 遍历所有WAL文件
	for _, file := range files {
		if file.IsDir() {
			continue // 跳过目录
		}

		name := file.Name()
		if !strings.HasPrefix(name, "data_") || !strings.HasSuffix(name, ".wal") {
			continue
		}

		fileIdStr := strings.TrimPrefix(name, "data_")
		fileIdStr = strings.TrimSuffix(fileIdStr, ".wal")

		fileId, err := strconv.ParseInt(fileIdStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid wal file name %s: %v", name, err)
		}

		b.fileIds = append(b.fileIds, fileId)
	}

	// 如果没有找到任何WAL文件，创建一个新的
	if len(b.fileIds) == 0 {
		walFile := filepath.Join(walDir, getWalFileName(0))
		activeWal, err := wal.NewWAL(walFile, 0)
		if err != nil {
			return fmt.Errorf("failed to create initial WAL file: %v", err)
		}
		b.activeWal = activeWal
		return nil
	}

	// 按文件id排序
	sort.Slice(b.fileIds, func(i, j int) bool {
		return b.fileIds[i] < b.fileIds[j]
	})

	// 打开所有WAL文件
	for i, fileId := range b.fileIds {
		walFile := filepath.Join(walDir, getWalFileName(fileId))
		currWal, err := wal.NewWAL(walFile, fileId)
		if err != nil {
			return fmt.Errorf("failed to open wal file %s: %v", walFile, err)
		}
		if err := currWal.LoadWal(b.curIndex); err != nil {
			return fmt.Errorf("failed to load wal file %s: %v", walFile, err)
		}

		if i == len(b.fileIds)-1 {
			b.activeWal = currWal
			b.fileId = fileId
		} else {
			b.olderWal[fileId] = currWal
		}
	}

	return nil
}

// getWalDir 获取WAL目录路径
func getWalDir(dirPath string) string {
	return filepath.Join(dirPath, "data_wal")
}

// getWalFileName 获取WAL文件名
func getWalFileName(fileId int64) string {
	return fmt.Sprintf("data_%09d.wal", fileId)
}

func (b *Bitcask) getWalFile(fileId int64) (*wal.WAL, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if fileId == b.fileId {
		return b.activeWal, nil
	}
	targetWal, ok := b.olderWal[fileId]
	if !ok {
		return nil, fmt.Errorf("wal file %d not found", fileId)
	}
	return targetWal, nil
}

func (b *Bitcask) checkOverFlow() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.activeWal.GetOffset() > b.config.MaxFileSize
}

func (b *Bitcask) tryCreateNewWalFile() error {

	if !b.checkOverFlow() {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.activeWal.Sync(); err != nil {
		return fmt.Errorf("failed to sync active wal: %v", err)
	}
	b.olderWal[b.fileId] = b.activeWal
	b.fileId++
	walFile := filepath.Join(getWalDir(b.config.DirPath), getWalFileName(b.fileId))
	newWal, err := wal.NewWAL(walFile, b.fileId)
	if err != nil {
		return fmt.Errorf("failed to create new wal file %s: %v", walFile, err)
	}
	b.activeWal = newWal

	return nil
}

func (b *Bitcask) Put(key []byte, value []byte) error {
	pos, err := b.activeWal.Append(key, value, record.RecordTypeNormal)
	if err != nil {
		return fmt.Errorf("failed to append record to wal: %v", err)
	}
	if err := b.curIndex.Put(key, pos); err != nil {
		return fmt.Errorf("failed to put key to index: %v", err)
	}
	if err := b.tryCreateNewWalFile(); err != nil {
		return fmt.Errorf("failed to create new wal file: %v", err)
	}
	return nil
}

func (b *Bitcask) Del(key []byte) error {
	_, err := b.activeWal.Append(key, nil, record.RecordTypeDeleted)
	if err != nil {
		return fmt.Errorf("failed to append record to wal: %v", err)
	}
	if err := b.curIndex.Delete(key); err != nil {
		return fmt.Errorf("failed to delete key from index: %v", err)
	}
	if err := b.tryCreateNewWalFile(); err != nil {
		return fmt.Errorf("failed to create new wal file: %v", err)
	}
	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, bool) {
	pos, err := b.curIndex.Get(key)
	if err != nil {
		return nil, false
	}
	if pos == nil {
		return nil, false
	}
	walFile, err := b.getWalFile(pos.FileID)
	if err != nil {
		return nil, false
	}
	value, err := walFile.ReadAt(pos.Offset, pos.Size)
	if err != nil {
		return nil, false
	}
	header := record.FromBytes(value)
	if header.RecordType == record.RecordTypeDeleted {
		return nil, false
	}
	return header.Value, true
}

func (b *Bitcask) Close() error {
	for _, wal := range b.olderWal {
		wal.Close()
	}
	b.activeWal.Close()
	return nil
}
func (b *Bitcask) Show() {
	iter := b.curIndex.Iterator()
	for ; iter.Valid(); iter.Next() {
		key, pos := iter.Key(), iter.Value()
		fmt.Println("key", string(key), "pos", pos)
		walFile, err := b.getWalFile(pos.FileID)
		if err != nil {
			fmt.Println("failed to get wal file", err)
			continue
		}
		value, err := walFile.ReadAt(pos.Offset, pos.Size)
		if err != nil {
			fmt.Println("failed to read wal file", err)
			continue
		}
		header := record.FromBytes(value)
		fmt.Println("header", header)
	}
}
