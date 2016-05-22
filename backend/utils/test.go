package utils

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"
)

const (
	LOG_LEVEL_INFO  = 0
	LOG_LEVEL_WARN  = 3
	LOG_LEVEL_FATAL = 5
)

var (
	LOG_LEVEL = LOG_LEVEL_INFO
)

func RandBytes(length int) []byte {
	rand.Seed(int64(time.Now().Nanosecond()))
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		tmp := rand.Int() % 62
		switch {
		case tmp < 26:
			buf[i] = byte('a' + tmp)
		case tmp < 52:
			buf[i] = byte('A' + tmp - 26)
		default:
			buf[i] = byte('0' + tmp - 52)
		}
	}
	return buf
}

func Fatal(args ...interface{}) {
	if LOG_LEVEL > LOG_LEVEL_FATAL {
		return
	}
	fmt.Println(args...)
	fmt.Println()
	fmt.Println()

	buf := make([]byte, 1<<20)
	runtime.Stack(buf, true)
	fmt.Printf("\n%s", buf)
	os.Exit(-1)
}

func Info(args ...interface{}) {
	if LOG_LEVEL > LOG_LEVEL_INFO {
		return
	}
	fmt.Print("[Info]: ")
	fmt.Println(args...)
}

func Warn(args ...interface{}) {
	if LOG_LEVEL > LOG_LEVEL_WARN {
		return
	}
	fmt.Print("[Warn]: ")
	fmt.Println(args...)
}

func Assert(assertion bool, args ...interface{}) {
	if assertion == false {
		Fatal(args...)
	}
}
