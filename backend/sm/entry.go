/*
	Entry.go 维护了SM中记录的结构.
	虽然提供了多版本, 但是由于SM并没有提供Update操作, 所以对于每条entry, 有且只有一个版本.
*/
package sm

import (
	"nyadb2/backend/dm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
)

const (
	_ENTRY_OF_XMIN = 0
	_ENTRY_OF_XMAX = _ENTRY_OF_XMIN + tm.LEN_XID
	_ENTRY_DATA    = _ENTRY_OF_XMAX + tm.LEN_XID
)

/*
	entry的二进制结构:
	[XMIN] [XMAX] [Data]
*/
type entry struct {
	selfUUID utils.UUID
	dataitem dm.Dataitem

	sm *serializabilityManager
}

func newEntry(sm *serializabilityManager, di dm.Dataitem, uuid utils.UUID) *entry {
	return &entry{
		selfUUID: uuid,
		sm:       sm,
		dataitem: di,
	}
}

func LoadEntry(sm *serializabilityManager, uuid utils.UUID) (*entry, bool, error) {
	di, ok, err := sm.dm.Read(uuid)
	if err != nil {
		return nil, false, err
	}
	if ok == false {
		return nil, false, nil
	}
	return newEntry(sm, di, uuid), true, nil
}

// WrapEntryRaw 将xid和data包裹成entry的二进制数据.
func WrapEntryRaw(xid tm.XID, data []byte) []byte {
	raw := make([]byte, _ENTRY_DATA+len(data))
	tm.PutXID(raw[_ENTRY_OF_XMIN:], xid)
	copy(raw[_ENTRY_DATA:], data)
	return raw
}

// Release 释放一个entry的引用
func (e *entry) Release() {
	e.sm.ReleaseEntry(e)
}

// Remove 将entry从内存中彻底释放
func (e *entry) Remove() {
	e.dataitem.Release()
}

// Data 以拷贝的形式返回entry当前的内容
func (e *entry) Data() []byte {
	e.dataitem.RLock()
	defer e.dataitem.RUnlock()
	data := make([]byte, len(e.dataitem.Data())-_ENTRY_DATA)
	copy(data, e.dataitem.Data()[_ENTRY_DATA:])
	return data
}

func (e *entry) XMIN() tm.XID {
	e.dataitem.RLock()
	defer e.dataitem.RUnlock()
	return tm.ParseXID(e.dataitem.Data()[_ENTRY_OF_XMIN:])
}

func (e *entry) XMAX() tm.XID {
	e.dataitem.RLock()
	defer e.dataitem.RUnlock()
	return tm.ParseXID(e.dataitem.Data()[_ENTRY_OF_XMAX:])
}

func (e *entry) SetXMAX(xid tm.XID) {
	e.dataitem.Before()
	defer e.dataitem.After(xid)
	tm.PutXID(e.dataitem.Data()[_ENTRY_OF_XMAX:], xid)
}
