/*
   logger 负责日志文件的读写.

   日志文件的整体格式如下:
   [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]

   其中[BadTail]表示的是最后一条错误的日志, 当然, 有可能并不存在[BadTail].
   [XChecksum] 表示的是对Log1到LogN的所有日志计算的Checksum. 类型为uint32.

   	每条日志的二进制格式如下:
   	[Checksum] uint32 4bytes // 该条记录的Checksum, 计算过程只包含data
   	[Size] uint32 4bytes // 仅包含data部分
   	[Data] size

    每次插入一条Log后, 就会对XChecksum做一次更新.
    由于"插入Log->更新XChecksum"这个过程不能保证原子性, 所以如果在期间发生了错误, 那么整个
    日志文件将会被判断为失效.
*/
package logger

import (
	"errors"
	"nyadb2/backend/utils"
	"os"
	"sync"
)

type Logger interface {
	Log(data []byte)
	Truncate(x int64) error
	Next() ([]byte, bool) // 读取一条日志, 并将指针移到下一条的位置.
	Rewind()              // 将日志指针移动到第一条日志的位置.
	Close()
}

var (
	ErrBadLogFile = errors.New("Bad log file.")
)

const (
	_SEED = 13331

	_OF_SIZE     = 0
	_OF_CHECKSUM = _OF_SIZE + 4
	_OF_DATA     = _OF_CHECKSUM + 4

	SUFFIX_LOG = ".log"
)

type logger struct {
	file *os.File
	lock sync.Mutex

	pos       int64 // 当前日志指针的位置
	fileSize  int64 // 该字段只有初始化的时候会被更新一次, Log操作不会更新它
	xChecksum uint32
}

func Open(path string) *logger {
	file, err := os.OpenFile(path+SUFFIX_LOG, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	lg := new(logger)
	lg.file = file

	err = lg.init()
	if err != nil {
		panic(err)
	}

	return lg
}

func Create(path string) *logger {
	file, err := os.OpenFile(path+SUFFIX_LOG, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

	xChecksum := utils.Uint32ToRaw(0)
	_, err = file.Write(xChecksum)
	if err != nil {
		panic(err)
	}
	err = file.Sync()
	if err != nil {
		panic(err)
	}

	lg := new(logger)
	lg.file = file
	lg.xChecksum = 0

	return lg
}

// updateXChecksum 更新XChecksum, 在之前该方法前, 需要上锁.
func (lg *logger) updateXChecksum(log []byte) {
	lg.xChecksum = calChecksum(lg.xChecksum, log)
	_, err := lg.file.WriteAt(utils.Uint32ToRaw(lg.xChecksum), 0)
	if err != nil {
		panic(err)
	}
	err = lg.file.Sync()
	if err != nil {
		panic(err)
	}
}

func (lg *logger) Log(data []byte) {
	log := wrapLog(data)

	lg.lock.Lock()
	defer lg.lock.Unlock()

	_, err := lg.file.Write(log)
	if err != nil {
		panic(err) // 如果logger出错, 那么DB是不能够继续进行下去的, 因此直接panic
	}

	// Sync()会在updateXChecksum内进行
	lg.updateXChecksum(log)
}

func wrapLog(data []byte) []byte {
	log := make([]byte, len(data)+_OF_DATA)
	utils.PutUint32(log[_OF_SIZE:], uint32(len(data)))
	copy(log[_OF_DATA:], data)
	checksum := calChecksum(0, data)
	utils.PutUint32(log[_OF_CHECKSUM:], checksum)
	return log
}

func calChecksum(accumulation uint32, data []byte) uint32 {
	for _, b := range data {
		accumulation = accumulation*_SEED + uint32(b)
	}
	return accumulation
}

func (lg *logger) Truncate(x int64) error {
	lg.lock.Lock()
	defer lg.lock.Unlock()
	return lg.file.Truncate(x)
}

func (lg *logger) Rewind() {
	lg.pos = 4
}

func (lg *logger) next() ([]byte, bool, error) {
	if lg.pos+_OF_DATA >= lg.fileSize {
		return nil, false, nil
	}

	tmp := make([]byte, 4)
	_, err := lg.file.ReadAt(tmp, lg.pos)
	if err != nil {
		return nil, false, err
	}

	size := int64(utils.ParseUint32(tmp))
	if lg.pos+size+_OF_DATA > lg.fileSize {
		return nil, false, nil // bad tail
	}

	log := make([]byte, _OF_DATA+size)
	_, err = lg.file.ReadAt(log, lg.pos)
	if err != nil {
		return nil, false, err
	}

	checksum1 := calChecksum(0, log[_OF_DATA:])
	checksum2 := utils.ParseUint32(log[_OF_CHECKSUM:])
	if checksum1 != checksum2 {
		return nil, false, nil // bad tail
	}

	lg.pos += int64(len(log))

	return log, true, nil
}

func (lg *logger) Next() ([]byte, bool) {
	lg.lock.Lock()
	defer lg.lock.Unlock()

	log, ok, err := lg.next()
	if err != nil {
		panic(err)
	}

	if ok == false {
		return nil, false
	}

	return log[_OF_DATA:], true
}

func (lg *logger) init() error {
	info, err := lg.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	if fileSize < 4 {
		return ErrBadLogFile
	}

	raw := make([]byte, 4)
	_, err = lg.file.ReadAt(raw, 0)
	if err != nil {
		return err
	}
	xChecksum := utils.ParseUint32(raw)

	lg.fileSize = fileSize
	lg.xChecksum = xChecksum

	return lg.checkAndRemoveTail()
}

// checkAndRemoveTail 检查xChecksum并且移除bad tail
func (lg *logger) checkAndRemoveTail() error {
	lg.Rewind()

	var xChecksum uint32
	for {
		log, ok, err := lg.next()
		if err != nil {
			return err
		}
		if ok == false {
			break
		}
		xChecksum = calChecksum(xChecksum, log)
	}

	// if xChecksum == lg.xChecksum {
	if true {
		/*
			// TODO
			由于更新xCheckSum的时候数据库发生崩溃, 则会导致整个log文件不能使用.
			所以暂时放弃xCheckSum, 之后将xCheckSum改为由booter管理.
		*/
		err := lg.file.Truncate(lg.pos) // 去掉bad tail
		if err != nil {
			return err
		}
		_, err = lg.file.Seek(lg.pos, 0)
		if err != nil {
			return err
		}
		lg.Rewind()
		return nil
	} else {
		return ErrBadLogFile
	}
}

func (lg *logger) Close() {
	err := lg.file.Close()
	if err != nil {
		panic(err)
	}
}
