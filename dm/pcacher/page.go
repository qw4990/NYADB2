/*
	page.go 实现了关于Page的逻辑和接口.

	其中需要注意的是两个协议

	Page更新协议:
		在对Page做任何的更新之前, 一定需要吸纳调用Dirty().

	Page释放协议:
		在对Page操作完之后, 一定要调用Release()释放掉该页.
*/
package pcacher

import "sync"

type Page interface {
	Pgno() Pgno   // 取得页号
	Data() []byte // 取得页内容, 以共享的方式
	Dirty()       // 将该页设置为脏页
	Release()     // 释放该页

	Lock()
	Unlock()
}

type page struct {
	pgno  Pgno
	data  []byte
	dirty bool
	lock  sync.Mutex

	pc *pcacher
}

func NewPage(pgno Pgno, data []byte, pc *pcacher) *page {
	return &page{
		pgno: pgno,
		data: data,
		pc:   pc,
	}
}

func (p *page) Unlock() {
	p.lock.Unlock()
}

func (p *page) Lock() {
	p.lock.Lock()
}

func (p *page) Release() {
	p.pc.release(p)
}

func (p *page) Dirty() {
	p.dirty = true
}

func (p *page) Pgno() Pgno {
	return p.pgno
}

func (p *page) Data() []byte {
	return p.data
}
