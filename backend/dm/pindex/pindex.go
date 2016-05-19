/*
   pindex 实现了对(Pgno, FreeSpace)键值对的缓存.
   其中FreeSpace表示的是Pgno这一页还剩多少空间可用.
   pindex存在目的在于, 当DM执行Insert操作时, 可用根据数据大小, 快速的选出有适合空间的页.

   目前pindex的算法非常简单.
   设置 threshold := pcacher.PAGE_SIZE / _NO_INTERVALS,
   然后划分出_NO_INTERVALS端区间, 分别表示FreeSpace大小为:
   [0, threshold), [threshold, 2*threshold), ...
   每个区间内的页用链表组织起来.
*/
package pindex

import (
	"container/list"
	"nyadb2/backend/dm/pcacher"
	"sync"
)

const (
	_NO_INTERVALS = 40
	_THRESHOLD    = pcacher.PAGE_SIZE / _NO_INTERVALS
)

type Pindex interface {
	/*
	   Add将该键值对加入到Pindex中.
	*/
	Add(pgno pcacher.Pgno, freeSpace int)
	/*
	   Select为spaceSize选择适当的Pgno, 并暂时将Pgno从Pindex中移除.
	*/
	Select(spaceSize int) (pcacher.Pgno, int, bool)
}

type pindex struct {
	lock  sync.Mutex
	lists [_NO_INTERVALS + 1]list.List
}

type pair struct {
	pgno      pcacher.Pgno
	freeSpace int
}

func NewPindex() *pindex {
	return &pindex{
		lists: [_NO_INTERVALS + 1]list.List{},
	}
}

func (pi *pindex) Add(pgno pcacher.Pgno, freeSpace int) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	no := freeSpace / _THRESHOLD
	pi.lists[no].PushBack(&pair{pgno, freeSpace})
}

func (pi *pindex) Select(spaceSize int) (pcacher.Pgno, int, bool) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	no := spaceSize / _THRESHOLD
	if no < _NO_INTERVALS {
		no++
	}
	for no <= _NO_INTERVALS {
		if pi.lists[no].Len() == 0 {
			no++
			continue
		}
		e := pi.lists[no].Front()
		v := pi.lists[no].Remove(e)
		pr := v.(*pair)
		return pr.pgno, pr.freeSpace, true
	}
	return 0, 0, false
}
