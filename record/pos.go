package record

import "fmt"

// Pos 记录位置
type Pos struct {
	FileID int64 // 文件ID
	Offset int64 // 偏移量
	Size   int64 // 大小
}

// String 实现 Stringer 接口
func (p *Pos) String() string {
	return fmt.Sprintf("FileID: %d, Offset: %d, Size: %d", p.FileID, p.Offset, p.Size)
}
