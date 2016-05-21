package client

import (
	"bufio"
	"fmt"
	"os"
)

type Shell interface {
	Run()
}

type shell struct {
	client Client
}

func NewShell(client Client) *shell {
	return &shell{
		client: client,
	}
}

func (s *shell) Run() {
	defer s.client.Close()

	termReader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(":> ")
		line, err := termReader.ReadBytes('\n')
		stat := line[:len(line)-1]
		if err != nil {
			fmt.Println(err)
			break
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

		result, err := s.client.Execute(stat)
		if err != nil {
			fmt.Println("Err: ", err)
		} else {
			fmt.Println(string(result))
		}
	}
}
