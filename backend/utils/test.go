package utils

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
)

func RandBytes(length int) []byte {
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
	fmt.Println(args...)
	fmt.Println()
	fmt.Println()

	buf := make([]byte, 1<<20)
	runtime.Stack(buf, true)
	fmt.Printf("\n%s", buf)
	os.Exit(-1)
}

func Info(args ...interface{}) {
	fmt.Print("[Info]: ")
	fmt.Println(args...)
}

func Assert(assertion bool, args ...interface{}) {
	if assertion == false {
		Fatal(args...)
	}
}
