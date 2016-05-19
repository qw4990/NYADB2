package dm_test

import (
	"bytes"
	"math/rand"
	"nyadb2/backend/dm"
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
	"testing"
)

var (
	uids0, uids1 []utils.UUID
	uidsLock     sync.Mutex
	wg           sync.WaitGroup
)

func initUids() {
	uids0 = make([]utils.UUID, 0)
	uids1 = make([]utils.UUID, 0)
}

func worker(dm0, dm1 dm.DataManager, noTasks int, insertRation int) {
	defer wg.Done()
	for i := 0; i < noTasks; i++ {
		op := rand.Int() % 100
		if op < insertRation { // Insert
			data := utils.RandBytes(50)
			u0, err := dm0.Insert(0, data)
			if err != nil {
				/*
				 err只会是ErrCacheFull或者ErrBusy之类的错误,
				 该种错误不会对DM产生实质的影响, 因此直接continue即可.
				 下面的continue同理.
				*/
				continue
			}
			u1, _ := dm1.Insert(0, data)

			uidsLock.Lock()
			uids0 = append(uids0, u0)
			uids1 = append(uids1, u1)
			uidsLock.Unlock()
		} else { // Check and Update
			uidsLock.Lock()
			if len(uids0) == 0 {
				uidsLock.Unlock()
				continue
			}
			tmp := rand.Int() % len(uids0)
			u0 := uids0[tmp]
			u1 := uids1[tmp]
			uidsLock.Unlock()

			data0, _, err := dm0.Read(u0)
			if err != nil {
				continue
			}
			data1, _, _ := dm1.Read(u1)

			data0.RLock()
			data1.RLock()
			if bytes.Compare(data0.Data(), data1.Data()) != 0 {
				utils.Fatal("error")
			}
			data1.RUnlock()
			data0.RUnlock()

			newData := utils.RandBytes(50)
			data0.Before()
			data1.Before()
			copy(data0.Data(), newData)
			copy(data1.Data(), newData)
			data0.After(0)
			data1.After(0)

			data0.Release()
			data1.Release()
		}
	}
}

func TestDMSingle(t *testing.T) {
	tm0 := tm.CreateMockXIDFile("abc")
	dm0 := dm.CreateDB("/tmp/TESTDMSingle", pcacher.PAGE_SIZE*10, tm0)
	mdm := dm.CreateMockDB("ttt", 0, tm0)

	noTasks := 10000
	wg.Add(1)
	initUids()
	go worker(dm0, mdm, noTasks, 50)
	wg.Wait()
}

func TestDMMulti(t *testing.T) {
	tm0 := tm.CreateMockXIDFile("abc")
	dm0 := dm.CreateDB("/tmp/TestDMMulti", pcacher.PAGE_SIZE*10, tm0)
	mdm := dm.CreateMockDB("ttt", 0, tm0)

	noTasks := 1000
	noWorkers := 100
	initUids()
	wg.Add(noWorkers)
	for i := 0; i < noWorkers; i++ {
		go worker(dm0, mdm, noTasks, 50)
	}
	wg.Wait()
}

func TestRecoverySingle(t *testing.T) {
	tm0 := tm.CreateXIDFile("/tmp/TestRecoverySingle")
	dm0 := dm.CreateDB("/tmp/TestRecoverySingle", pcacher.PAGE_SIZE*10, tm0)
	mdm := dm.CreateMockDB("ttt", 0, tm0)

	initUids()
	wg.Add(1)
	worker(dm0, mdm, 10000, 50)

	// 此处不调用dm0.Close(), 立即重新打开DB, 触发Recovery.
	dm0 = dm.OpenDB("/tmp/TestRecoverySingle", pcacher.PAGE_SIZE*10, tm0)
	// 然后继续用worker进行测试
	wg.Add(1)
	worker(dm0, mdm, 100000, 0)
}
