package main

import (
	"math/rand"
	"nyadb2/backend/utils"
	"os"
	"strconv"
)

func main() {
	for i := 0; i < 5; i++ {
		genInputC(i, 200)
	}
}

func genInputC(id, noTasks int) {
	file, _ := os.OpenFile("./Cinput"+strconv.Itoa(id)+".input", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	file.Write([]byte("begin\n"))
	defer func() {
		file.Write([]byte("commit\n"))
		file.Write([]byte("exit\n"))
		file.Sync()
	}()

	for i := 0; i < noTasks; i++ {
		sql := genSQLC(id*noTasks+i) + "\n"
		file.Write([]byte(sql))
	}
}

func genSQLC(i int) string {
	sql := "insert into student values " + "MAGIC" + string(utils.RandBytes(50)) + " " +
		strconv.Itoa(i) + " " +
		utils.Int32ToStr(int32(rand.Uint32()%1000000000)) + " "
	return sql
}
