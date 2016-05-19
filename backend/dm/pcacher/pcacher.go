/*
   pcacher 实现了对页的缓存.
   实际上pcacher已经将缓存的逻辑托管给了cacher.Cacher了.
   所以在pcacher中, 只需要实现对磁盘操作的部分逻辑.
*/
package pcacher

import (
	"errors"
	"fmt"
	"nyadb2/backend/utils"
	"nyadb2/backend/utils/cacher"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrMemTooSmall = errors.New("Memory is too small.")
)

const (
	PAGE_SIZE = 1 << 13
	_MEM_LIM  = 10
)

type Pcacher interface {
	/*
		该函数返回一个Pgno, 而不是一个Page, 原因是:
		如果返回一个Page, 则实际上整个过程是需要两步, 1)创建新页, 2)从cache中取得新页.
		问题出在, 如果1)成功, 而2)因为cache full而失败, 则将不能返回Page, 导致新页不能马上
		被利用, 因此还不如不要2)过程, 直接返回Pgno.
		将2)过程交给用户去调用GetPage()
	*/
	NewPage(initData []byte) Pgno    // 新创建一页, 返回新页页号
	GetPage(pgno Pgno) (Page, error) // 根据叶号取得一页
	Close()

	/*
		下面的方法只有在Recovery的时候才会被调用.
		下面的方法不需要支持并发.
	*/
	TruncateByPgno(maxPgno Pgno) // 将DB扩充为maxPgno这么多页的空间大小
	NoPages() int                // 返回DB中一共有多少页
}

type pcacher struct {
	file     *os.File
	fileLock sync.Mutex

	noPages uint32

	c cacher.Cacher
}

func CreateCacheFile(path string, mem int64) *pcacher {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	return newPcacher(file, mem)
}

func OpenCacheFile(path string, mem int64) *pcacher {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	return newPcacher(file, mem)
}

func newPcacher(file *os.File, mem int64) *pcacher {
	if mem/PAGE_SIZE < _MEM_LIM {
		panic(ErrMemTooSmall)
	}

	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	size := info.Size()

	p := new(pcacher)
	options := new(cacher.Options)
	options.Get = p.getForCacher
	options.MaxHandles = uint32(mem / PAGE_SIZE)
	options.Release = p.releaseForCacher
	c := cacher.NewCacher(options)
	p.c = c
	p.file = file
	p.noPages = uint32(size / PAGE_SIZE)

	return p
}

func (p *pcacher) Close() {
	p.c.Close()
}

func (p *pcacher) NewPage(initData []byte) Pgno {
	// 将noPages增加1, 且预留出一个页号的位置.
	pgno := Pgno(atomic.AddUint32(&p.noPages, 1))
	pg := NewPage(pgno, initData, nil)
	p.flush(pg)
	return pgno
}

func (p *pcacher) GetPage(pgno Pgno) (Page, error) {
	uid := Pgno2UUID(pgno)
	underlying, err := p.c.Get(uid)
	if err != nil {
		return nil, err
	}
	return underlying.(*page), nil
}

// get 根据pgno从DB文件中读取页的内容, 并包裹成一页返回.
// get必须能够支持并发.
func (p *pcacher) getForCacher(uid utils.UUID) (interface{}, error) {
	pgno := UUID2Pgno(uid)
	offset := pageOffset(pgno)

	buf := make([]byte, PAGE_SIZE)
	p.fileLock.Lock()
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		fmt.Println(uid)
		panic(err) // 如果DB文件出了问题, 则应该立即停止.
	}
	p.fileLock.Unlock()

	pg := NewPage(pgno, buf, p)
	return pg, nil
}

// release 释放掉该页的内容, 也就是刷新该页, 然后从内存中释放掉.
func (p *pcacher) releaseForCacher(underlying interface{}) {
	pg := underlying.(*page)
	if pg.dirty == true {
		p.flush(pg)
		pg.dirty = false
	}
}

func (p *pcacher) release(pg *page) {
	p.c.Release(Pgno2UUID(pg.pgno))
}

// flush 刷新某一页的内容到DB文件.
// 因为flush为被release调用, 所以flush也必须是支持并发的.
func (p *pcacher) flush(pg *page) {
	pgno := pg.pgno
	offset := pageOffset(pgno)

	p.fileLock.Lock()
	defer p.fileLock.Unlock()
	_, err := p.file.WriteAt(pg.data, offset)
	if err != nil {
		panic(err) // 如果DB文件出现了问题, 那么直接结束.
	}
	err = p.file.Sync()
	if err != nil {
		panic(err)
	}
}

func (p *pcacher) TruncateByPgno(maxPgno Pgno) {
	size := pageOffset(maxPgno + 1)
	err := p.file.Truncate(size)
	if err != nil {
		panic(err)
	}
	p.noPages = uint32(maxPgno)
}

func (p *pcacher) NoPages() int {
	return int(p.noPages)
}

func pageOffset(pgno Pgno) int64 {
	// 页号从1开始, 所以需要-1
	return int64(pgno-1) * PAGE_SIZE
}
