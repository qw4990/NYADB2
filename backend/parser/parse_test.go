package parser

import (
	"fmt"
	"os"
	"testing"

	"nyadb2/backend/parser/statement"
)

func TestCreate(t *testing.T) {
	stat := `
    create table student
    id int32,
    name string,
    uid int64,
    (index name id uid)
    `
	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	create := result.(*statement.Create)
	fmt.Println(create.TableName)
	for i := 0; i < len(create.FieldName); i++ {
		fmt.Println(create.FieldName[i], " : ", create.FieldType[i])
	}
	fmt.Println("index: ", create.Index)
	fmt.Println()
	fmt.Println("=========================")
}

func TestBegin(t *testing.T) {
	stat := `
        begin isolation level read committed`
	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(result)
	fmt.Println()
	fmt.Println("===========================")
}

func TestRead(t *testing.T) {
	stat := `
        read name, id, strudeng from student where id > 1 and id < 4
        `
	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(result)
	fmt.Println()
	fmt.Println("===========================")
}

func TestInsert(t *testing.T) {
	stat := `
        insert into student values 5 "Zhang Yuanjia" 22
        `
	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(result)
	fmt.Println()
	fmt.Println("===========================")
}

func TestDelete(t *testing.T) {
	stat := `
        delete from student where name = "Zhang Yuanjia"
        `
	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(result)
	fmt.Println()
	fmt.Println("===========================")
}

func TestShow(t *testing.T) {
	stat := `
	show
	        `
	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(result)
	fmt.Println()
	fmt.Println("=show==========================")
}

func TestUpdate(t *testing.T) {
	stat := `
		update student set name = "ZYJ" where id = 5
	`

	result, err := Parse([]byte(stat))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(result)
	fmt.Println()
	fmt.Println("=update==========================")
}
