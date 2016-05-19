package tm_test

import (
	"fmt"
	"math/rand"
	"nyadb2/backend/tm"
	"os"
	"sync"
	"testing"
)

func TestMultiThread(t *testing.T) {
	tmger := tm.CreateXIDFile("/tmp/tranmger_test.xid")

	transCnt := 0
	transMap := make(map[tm.XID]byte)
	lock := new(sync.Mutex)
	noWorkers := 50
	noWorks := 3000
	waitGroup := new(sync.WaitGroup)

	worker := func() {
		inTrans := false
		var tranXID tm.XID
		for i := 0; i < noWorks; i++ {
			op := rand.Int() % 6
			if op == 0 { // Begin or Terminate
				lock.Lock()
				if inTrans == false { // Begin a new transaction
					xid := tmger.Begin()
					transMap[xid] = 0 // Set xid to active
					tranXID = xid
					inTrans = true
				} else {
					status := (rand.Int() % 2) + 1
					switch status {
					case 1: // commit
						tmger.Commit(tranXID)
					case 2: // abort
						tmger.Abort(tranXID)
					}
					transMap[tranXID] = byte(status) // set xid status
					inTrans = false
				}
				lock.Unlock()
			} else { // Check
				lock.Lock()
				if transCnt > 0 {
					xid := tm.XID((rand.Int() % transCnt) + 1)
					status := transMap[xid]
					var ok bool
					switch status {
					case 0: // active
						ok = tmger.IsActive(xid)
					case 1: // commited
						ok = tmger.IsCommited(xid)
					case 2: // aborted
						ok = tmger.IsAborted(xid)
					}
					if ok == false {
						fmt.Println("Check error!")
						os.Exit(-1)
					}
				}
				lock.Unlock()
			}
		}
		waitGroup.Done()
	}

	waitGroup.Add(noWorkers)
	for i := 0; i < noWorkers; i++ {
		go worker()
	}
	waitGroup.Wait()
}
