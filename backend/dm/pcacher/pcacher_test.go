package pcacher_test

import (
	"bytes"
	"math/rand"
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/utils"
	"nyadb2/backend/utils/cacher"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPcacherSimple(t *testing.T) {
	pc := pcacher.Create("/tmp/pcacher_simple_test.db", pcacher.PAGE_SIZE*50)
	for i := 0; i < 100; i++ {
		tmp := make([]byte, pcacher.PAGE_SIZE)
		pgno := pc.NewPage(tmp)
		pg, err := pc.GetPage(pgno)
		if err != nil {
			utils.Fatal(err)
		}
		pg.Dirty()
		pg.Data()[0] = byte(i)
		pg.Release()
	}
	pc.Close()

	pc = pcacher.Open("/tmp/pcacher_simple_test.db", pcacher.PAGE_SIZE*50)

	for i := 1; i <= 100; i++ {
		pg, err := pc.GetPage(pcacher.Pgno(i))
		if err != nil {
			utils.Fatal(err)
		}
		if pg.Data()[0] != byte(i-1) {
			utils.Fatal(err)
		}
		pg.Release()
	}
	pc.Close()
}

func TestPcacherMultiSimple(t *testing.T) {
	pc := pcacher.Create("/tmp/pcacher_multisimple_test.db", pcacher.PAGE_SIZE*50)

	var noPages uint32
	wg := sync.WaitGroup{}
	noWorkers := 200

	wg.Add(noWorkers)

	worker := func(id int) {
		for i := 0; i < 80; i++ {
			op := rand.Int() % 20
			if op == 0 { // New Page
				data := utils.RandBytes(pcacher.PAGE_SIZE)
				pgno := pc.NewPage(data)
				pg, err := pc.GetPage(pgno)
				if err != nil {
					if err == cacher.ErrCacheFull {
						continue
					}
					utils.Fatal(err)
				}

				atomic.AddUint32(&noPages, 1)
				pg.Release()
			} else if op < 20 { // Read
				mod := atomic.LoadUint32(&noPages)
				if mod == 0 {
					continue
				}
				pgno := pcacher.Pgno((rand.Uint32() % mod) + 1)
				pg, err := pc.GetPage(pgno)
				if err != nil {
					if err == cacher.ErrCacheFull {
						continue
					}
					utils.Fatal(err)
				}

				pg.Release()
			}
		}
		wg.Done()
	}
	for i := 0; i < noWorkers; i++ {
		go worker(i)
	}

	wg.Wait()
}

func TestPcacherMulti(t *testing.T) {
	pc := pcacher.Create("/tmp/pcacher_multi_test.db", pcacher.PAGE_SIZE*10)
	mpc := pcacher.NewMock()
	lockNew := sync.Mutex{}

	var noPages uint32
	wg := sync.WaitGroup{}
	noWorkers := 30

	wg.Add(noWorkers)

	worker := func(id int) {
		for i := 0; i < 1000; i++ {
			op := rand.Int() % 20
			if op == 0 { // New Page
				data := utils.RandBytes(pcacher.PAGE_SIZE)

				lockNew.Lock() // 为了让pc和mpc同步更新, 多个线程同时new, 页号可能会出错.
				pc.NewPage(data)
				mpc.NewPage(data)
				lockNew.Unlock()

				atomic.AddUint32(&noPages, 1)
			} else if op < 10 { // Check
				mod := atomic.LoadUint32(&noPages)
				if mod == 0 {
					continue
				}

				pgno := pcacher.Pgno((rand.Uint32() % mod) + 1)
				pg, err := pc.GetPage(pgno)
				if err != nil {
					if err == cacher.ErrCacheFull {
						continue
					}
					utils.Fatal(err)
				}

				mpg, _ := mpc.GetPage(pgno)

				pg.Lock()
				if bytes.Compare(mpg.Data(), pg.Data()) != 0 {
					utils.Fatal("error")
				}
				pg.Unlock()

				pg.Release()
			} else { // Update
				mod := atomic.LoadUint32(&noPages)
				if mod == 0 {
					continue
				}

				pgno := pcacher.Pgno((rand.Uint32() % mod) + 1)
				pg, err := pc.GetPage(pgno)
				if err != nil {
					if err == cacher.ErrCacheFull {
						continue
					}
					utils.Fatal(err)
				}
				mpg, _ := mpc.GetPage(pgno)
				newData := utils.RandBytes(pcacher.PAGE_SIZE)

				pg.Lock()
				mpg.Dirty()
				copy(mpg.Data(), newData)
				pg.Dirty()
				copy(pg.Data(), newData)
				pg.Unlock()

				pg.Release()
			}
		}
		wg.Done()
	}
	for i := 0; i < noWorkers; i++ {
		go worker(i)
	}

	wg.Wait()
}
