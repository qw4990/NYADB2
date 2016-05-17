package logger

type Logger interface {
	Log(data []byte)
	Truncate(x int64) error
	Next() ([]byte, bool) // 读取一条日志, 并将指针移到下一条的位置.
	Rewind()              // 将日志指针移动到第一条日志的位置.
	Close() error
}
