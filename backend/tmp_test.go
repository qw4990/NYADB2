package main

import (
	"nyadb2/backend/dm"
	"nyadb2/backend/server"
	"nyadb2/backend/sm"
	"nyadb2/backend/tbm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"

	"sync"
	"time"
	"fmt"
)

const (
	_DEFAULT_PATH = "/tmp/nyadb"
	_DEFAULT_MEM  = (1 << 20) * 64 // 64MB
)

var (
	_CREATE_TABLE []byte = []byte("create table test_table id int32 (index id)")
	_INSERT       []byte = []byte("insert into test_table values 2333")
)

func testCreate() server.Executor {
	tm := tm.Create(_DEFAULT_PATH)
	dm := dm.Create(_DEFAULT_PATH, _DEFAULT_MEM, tm)
	sm := sm.NewSerializabilityManager(tm, dm)
	tbm := tbm.Create(_DEFAULT_PATH, sm, dm)
	exe := server.NewExecutor(tbm)
	utils.LOG_LEVEL = utils.LOG_LEVEL_FATAL
	exe.Execute(_CREATE_TABLE)
	return exe
}

func testInsert(exe server.Executor, noInsertions int, prt bool) {
	begin := time.Now().UnixNano()
	defer func() {
		end := time.Now().UnixNano()
		if prt {
			fmt.Println((end - begin) / 1000000, "ms")
		}
	}()

	for i := 0; i < noInsertions; i++ {
		exe.Execute(_INSERT)
	}
}

func TestInsertSingle10000() {
	fmt.Print("TestInsertSingle10000: ")
	e := testCreate()
	testInsert(e, 10000, true)
}

func TestInsertSingle100000() {
	fmt.Print("TestInsertSingle100000: ")
	e := testCreate()
	testInsert(e, 100000, true)
}

func TestInsertSingle1000000() {
	fmt.Print("TestInsertSingle1000000: ")
	e := testCreate()
	testInsert(e, 1000000, true)
}

func TestInsertSingle10000000() {
	fmt.Print("TestInsertSingle10000000: ")
	e := testCreate()
	testInsert(e, 10000000, true)
}

func testMultiInsert(tot, noWorkers int) {
	begin := time.Now().UnixNano()
	defer func() {
		end := time.Now().UnixNano()
		fmt.Println("Multi Tot:", (end - begin) / 1000000, "ms")
	}()

	e := testCreate()

	wg := sync.WaitGroup{}
	wg.Add(noWorkers)

	f := func(e server.Executor) {
		testInsert(e, tot/noWorkers, false)
		wg.Done()
	}

	for i := 0; i < noWorkers; i++ {
		go f(e)
	}

	wg.Wait()
}

func TestInsert10000000With2() {
	fmt.Print("TestInsert10000000With2: ")
	testMultiInsert(10000000, 2)
}

func TestInsert10000000With3() {
	fmt.Print("TestInsert10000000With3: ")
	testMultiInsert(10000000, 3)
}

func TestInsert10000000With4() {
	fmt.Print("TestInsert10000000With4: ")
	testMultiInsert(10000000, 4)
}

func TestInsert10000000With5() {
	fmt.Print("TestInsert10000000With5: ")
	testMultiInsert(10000000, 5)
}

func TestInsert10000000With10() {
	fmt.Print("TestInsert10000000With10: ")
	testMultiInsert(10000000, 10)
}

func TestInsert10000000With20() {
	fmt.Print("TestInsert10000000With20: ")
	testMultiInsert(10000000, 20)
}

func TestInsert10000000With30() {
	fmt.Print("TestInsert10000000With30: ")
	testMultiInsert(10000000, 30)
}

func TestInsert10000000With40() {
	fmt.Print("TestInsert10000000With40: ")
	testMultiInsert(10000000, 40)
}

func main() {
	TestInsertSingle10000()
	TestInsertSingle100000()
	TestInsertSingle1000000()
	TestInsertSingle10000000()

	TestInsert10000000With2()
	TestInsert10000000With3()
	TestInsert10000000With4()
	TestInsert10000000With5()
	TestInsert10000000With10()
	TestInsert10000000With20()
	TestInsert10000000With30()
	TestInsert10000000With40()
}
