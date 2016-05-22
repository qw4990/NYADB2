/*
   Dataitem 为DataEngine为上层模块提供的数据抽象
   上层模块需要根据地址， 向DataEngine请求对应的Dataitem
   然后通过Data方法， 取得DataItem实际内容

   下面是一些关于DataItem的协议.

 	数据共享:
		利用d.Data()得到的数据, 是内存共享的.

  	数据项修改协议:
   		上层模块在对数据项进行任何修改之前, 都必须调用d.Before(), 如果想撤销修改, 则再调用
		d.UnBefore(). 修改完成后, 还必须调用d.After(xid).
		DM会保证对Dataitem的修改是原子性的.

	数据项释放协议:
		上层模块不用数据项时, 必须调用d.Release()来将其释放
*/
package dm

import (
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
)

type Dataitem interface {
	Data() []byte     // Data 以共享形式返回该dataitem的数据内容
	UUID() utils.UUID // Handle 返回该dataitem的handle

	Before()
	UnBefore()
	After(xid tm.XID)
	Release()

	// 下面是DM为上层模块提供的针对DataItem的锁操作.
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

/*
   对DataItem的实际实现， 其结构如下：
   [Valid Flag]        [Data Size]          [Data]
   1 byte bool		   2 bytes uint16       *

   Data Size标示了该dataitem中实际存储的data长度
   Valid Flag现在只有两个值， 0表示该dataitem合法， 1表示非法
   xid和flag的存在原因请参考logs.go中描述的恢复机制
*/

const (
	_OF_VALID_FLAG = 0
	_OF_DATA_SIZE  = 1
	_OF_DATA       = 3
)

type dataitem struct {
	raw    []byte
	oldraw []byte

	rwlock sync.RWMutex

	dm  *dataManager
	uid utils.UUID
	pg  pcacher.Page
}

func WrapDataitemRaw(data []byte) []byte {
	raw := make([]byte, _OF_DATA+len(data))
	utils.PutUint16(raw[_OF_DATA_SIZE:], uint16(len(data)))
	copy(raw[_OF_DATA:], data)
	return raw
}

// UnValidRawDataitem 将raw表示的Dataitem标记为非法.
// 该函数只会被Recovery调用.
func InValidRawDataitem(raw []byte) {
	raw[_OF_VALID_FLAG] = byte(1)
}

// ParseDataitem 从pg的offset位移处, 解析出对应的dataitem
func ParseDataitem(pg pcacher.Page, offset Offset, dm *dataManager) *dataitem {
	raw := pg.Data()[offset:]
	size := utils.ParseUint16(raw[_OF_DATA_SIZE:])
	length := size + _OF_DATA
	uid := Address2UUID(pg.Pgno(), offset)

	di := &dataitem{
		raw:    raw[:length],
		oldraw: make([]byte, length),
		pg:     pg,
		uid:    uid,
		dm:     dm,
	}
	return di
}

func (di *dataitem) IsValid() bool {
	return di.raw[_OF_VALID_FLAG] == byte(0)
}

func (di *dataitem) Data() []byte {
	return di.raw[_OF_DATA:]
}
func (di *dataitem) UUID() utils.UUID {
	return di.uid
}
func (di *dataitem) Before() {
	di.rwlock.Lock()
	di.pg.Dirty()
	copy(di.oldraw, di.raw)
}
func (di *dataitem) UnBefore() {
	copy(di.raw, di.oldraw)
	di.rwlock.Unlock()
}
func (di *dataitem) After(xid tm.XID) {
	di.dm.logDataitem(xid, di)
	di.rwlock.Unlock()
}
func (di *dataitem) Release() {
	di.dm.ReleaseDataitem(di)
}
func (di *dataitem) Lock() {
	di.rwlock.Lock()
}
func (di *dataitem) Unlock() {
	di.rwlock.Unlock()
}
func (di *dataitem) RLock() {
	di.rwlock.RLock()
}
func (di *dataitem) RUnlock() {
	di.rwlock.RUnlock()
}
