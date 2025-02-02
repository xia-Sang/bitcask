package bitcask

type Config struct {
	DirPath        string // 数据存储目录
	MaxFileSize    int64  // 单个文件最大大小
	MaxKeyLength   int64  // 单个key最大长度
	MaxValueLength int64  // 单个value最大长度
	SyncWrite      bool   // 是否同步写入
	IndexType      string // 索引类型
}

func NewConfig() *Config {
	return &Config{
		DirPath:        "./data",
		MaxFileSize:    1024 * 1024 * 1024, // 1GB
		MaxKeyLength:   1024,
		MaxValueLength: 1024 * 1024, // 1MB
		SyncWrite:      true,
		IndexType:      "btree",
	}
}
