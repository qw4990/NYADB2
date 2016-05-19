package main

import (
	"bufio"
	"fmt"
	"net"
	"nyadb/transporter"
	"nyadb2/backend/utils"
	"os"
)

const (
	_NET     = "tcp"
	_ADDRESS = ":8080"
)

func main() {
	conn, err := net.Dial(_NET, _ADDRESS)
	if err != nil {
		panic(err)
	}

	tr := transporter.NewHexTransporter(conn)
	pr := transporter.NewProtocoler()
	pk := transporter.NewPackager(tr, pr)
	defer pk.Close()

	termReader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(":> ")
		line, err := termReader.ReadBytes('\n')
		stat := line[:len(line)-1]
		if err != nil {
			fmt.Println(err)
		}
		if string(stat) == "clear" {
			for i := 0; i < 80; i++ {
				fmt.Println()
			}
			continue
		}
		if string(stat) == "exit" || string(stat) == "quit" {
			break
		}

		pkg := transporter.NewPackage([]byte(stat), nil)

		err = pk.Send(pkg)
		if err != nil {
			utils.Info(err)
			break
		}

		rpkg, err := pk.Receive()
		if err != nil {
			utils.Info(err)
			break
		}

		if rpkg.Err() != nil {
			fmt.Println(rpkg.Err())
		} else {
			fmt.Println(string(rpkg.Data()))
		}
	}
}
