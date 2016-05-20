package parser

import (
	"fmt"
	"testing"
)

func TestToken(t *testing.T) {
	cmd := []byte("update student32 set name='ZYJ' where id = 5")
	tk := newTokener(cmd)
	for {
		token, err := tk.Peek()
		if token == "" {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(token)
		tk.Pop()
	}
}
