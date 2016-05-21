package im

import (
	"fmt"
	"math/rand"
	"nyadb2/backend/dm"
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
	"testing"
)

func TestTreeSingle(t *testing.T) {
	// dm := dm.CreateMockDB("", 0, nil)
	tm := tm.CreateMock("/tmp/TestTreeSingle")
	dm := dm.Create("/tmp/TestTreeSingle", pcacher.PAGE_SIZE*10, tm)

	root, _ := Create(dm)
	tree, _ := Load(root, dm)

	lim := 10000
	for i := lim - 1; i >= 0; i-- {
		tree.Insert(utils.UUID(i), utils.UUID(i))
	}

	for i := 0; i < lim; i++ {
		uids, _ := tree.Search(utils.UUID(i))
		if len(uids) != 1 {
			t.Fatal("Error")
		}
		if uids[0] != utils.UUID(i) {
			t.Fatal("Error")
		}
	}
}

func TestTreeMultiInsert(t *testing.T) {
	tm := tm.CreateMock("/tmp/TestTreeMultiInsert")
	dm := dm.Create("/tmp/TestTreeMultiInsert", pcacher.PAGE_SIZE*80, tm)
	root, _ := Create(dm)
	tree, _ := Load(root, dm)

	aMap := make(map[utils.UUID]int)
	aLock := sync.Mutex{}

	noInsertor := 50
	noReader := 50
	noTasks := 1000

	wg := sync.WaitGroup{}
	wg.Add(noInsertor + noReader)

	insertor := func() {
		for i := 0; i < noTasks; i++ {
			uid := utils.UUID(rand.Uint32())
			err := tree.Insert(uid, uid)
			if err != nil {
				continue
			}
			aLock.Lock()
			aMap[uid]++
			aLock.Unlock()
		}
		wg.Done()
		fmt.Println("insertor done.")
	}

	reader := func() {
		for i := 0; i < noTasks; i++ {
			key0, key1 := utils.UUID(rand.Uint32()), utils.UUID(rand.Uint32())
			if key0 > key1 {
				key0, key1 = key1, key0
			}
			if key1-key0 > 10000 {
				key1 = key0 + 10000
			}
			tree.SearchRange(key0, key1)
		}
		wg.Done()
		fmt.Println("reader done.")
	}

	for i := 0; i < noInsertor; i++ {
		go insertor()
	}

	for i := 0; i < noReader; i++ {
		go reader()
	}

	wg.Wait()

	fmt.Println("checker begin.")
	for key, cnt := range aMap {
		addrs, _ := tree.Search(key)
		if len(addrs) != cnt {
			t.Fatal("Error")
		}
	}
	fmt.Println("checker end.")
}
