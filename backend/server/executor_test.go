package server_test

import (
	"nyadb2/backend/dm"
	"nyadb2/backend/server"
	"nyadb2/backend/sm"
	"nyadb2/backend/tbm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
	"testing"
)

const (
	_DEFAULT_PATH = "/tmp/nyadb"
	_DEFAULT_MEM  = (1 << 20) * 64 // 64MB
)

var (
	_CREATE_TABLE []byte = []byte("create table test_table id int32 (index id)")
	_INSERT       []byte = []byte("insert into test_table values 2333")
)

func testCreate(t *testing.T) server.Executor {
	tm := tm.Create(_DEFAULT_PATH)
	dm := dm.Create(_DEFAULT_PATH, _DEFAULT_MEM, tm)
	sm := sm.NewSerializabilityManager(tm, dm)
	tbm := tbm.Create(_DEFAULT_PATH, sm, dm)
	exe := server.NewExecutor(tbm)
	utils.LOG_LEVEL = utils.LOG_LEVEL_FATAL
	exe.Execute(_CREATE_TABLE)
	return exe
}

func testInsert(t *testing.T, exe server.Executor, noInsertions int) {
	// begin := time.Now().UnixNano()
	// defer func() {
	// 	fmt.Println(time.Now())
	// 	end := time.Now().UnixNano()
	// 	fmt.Println(begin, " ", end)
	// 	fmt.Println((end - begin) / 1000000)
	// }()

	for i := 0; i < noInsertions; i++ {
		exe.Execute(_INSERT)
	}
}

func TestInsertSingle10000(t *testing.T) {
	e := testCreate(t)
	testInsert(t, e, 10000)
}

func TestInsertSingle100000(t *testing.T) {
	e := testCreate(t)
	testInsert(t, e, 100000)
}

func TestInsertSingle1000000(t *testing.T) {
	e := testCreate(t)
	testInsert(t, e, 1000000)
}

func TestInsertSingle10000000(t *testing.T) {
	e := testCreate(t)
	testInsert(t, e, 10000000)
}

func testMultiInsert(tot, noWorkers int, t *testing.T) {
	e := testCreate(t)

	wg := sync.WaitGroup{}
	wg.Add(noWorkers)

	f := func(t *testing.T, e server.Executor) {
		testInsert(t, e, tot/noWorkers)
		wg.Done()
	}

	for i := 0; i < noWorkers; i++ {
		go f(t, e)
	}

	wg.Wait()
}

func TestInsert10000000With2(t *testing.T) {
	testMultiInsert(10000000, 2, t)
}

func TestInsert10000000With3(t *testing.T) {
	testMultiInsert(10000000, 3, t)
}

func TestInsert10000000With4(t *testing.T) {
	testMultiInsert(10000000, 4, t)
}

func TestInsert10000000With5(t *testing.T) {
	testMultiInsert(10000000, 5, t)
}

func TestInsert10000000With10(t *testing.T) {
	testMultiInsert(10000000, 10, t)
}

func TestInsert10000000With20(t *testing.T) {
	testMultiInsert(10000000, 20, t)
}

func TestInsert10000000With30(t *testing.T) {
	testMultiInsert(10000000, 30, t)
}

func TestInsert10000000With40(t *testing.T) {
	testMultiInsert(10000000, 40, t)
}
