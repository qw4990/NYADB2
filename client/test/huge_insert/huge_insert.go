package main

import (
	"os"
	"strconv"
)

func main() {
	file, _ := os.OpenFile("./input.input", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)

	file.Write([]byte("create table x x int32 (index x)\n"))
	for i := 0; i < 30; i++ {
		file.Write([]byte("begin\n"))
		for j := 0; j < 1000; j++ {
			file.Write([]byte("insert into x values " + strconv.Itoa(i*1000+j) + "\n"))
		}
		file.Write([]byte("commit\n"))
	}
}
