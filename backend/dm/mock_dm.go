/*
   mock_dm 将全部的数据存于内存.
*/
package dm

import (
	"math/rand"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
)

type mockDI struct {
	data    []byte
	olddata []byte
	uid     utils.UUID
	rwlock  sync.RWMutex
}

func newMockDI(uid utils.UUID, data []byte) *mockDI {
	return &mockDI{
		data:    data,
		olddata: make([]byte, len(data)),
		uid:     uid,
	}
}

func (mdi *mockDI) Data() []byte {
	return mdi.data
}
func (mdi *mockDI) UUID() utils.UUID {
	return mdi.uid
}
func (mdi *mockDI) Before() {
	mdi.rwlock.Lock()
	copy(mdi.olddata, mdi.data)
}
func (mdi *mockDI) UnBefore() {
	copy(mdi.data, mdi.olddata)
	mdi.rwlock.Unlock()
}
func (mdi *mockDI) After(xid tm.XID) {
	mdi.rwlock.Unlock()
}
func (mdi *mockDI) Release() {
}
func (mdi *mockDI) Lock() {
	mdi.rwlock.Lock()
}
func (mdi *mockDI) Unlock() {
	mdi.rwlock.Unlock()
}
func (mdi *mockDI) RLock() {
	mdi.rwlock.RLock()
}
func (mdi *mockDI) RUnlock() {
	mdi.rwlock.RUnlock()
}

type mockDM struct {
	cache map[utils.UUID]*mockDI
	lock  sync.Mutex
}

func CreateMockDB(path string, mem int64, tm tm.TransactionManager) *mockDM {
	return &mockDM{
		cache: make(map[utils.UUID]*mockDI),
	}
}

func (mdm *mockDM) Read(uid utils.UUID) (Dataitem, bool, error) {
	mdm.lock.Lock()
	defer mdm.lock.Unlock()
	if _, ok := mdm.cache[uid]; ok == false {
		return nil, false, nil
	}
	return mdm.cache[uid], true, nil
}

func (mdm *mockDM) Insert(xid tm.XID, data []byte) (utils.UUID, error) {
	mdm.lock.Lock()
	defer mdm.lock.Unlock()
	var uid utils.UUID
	for {
		uid = utils.UUID(rand.Uint32())
		if uid == 0 {
			continue
		}
		if _, ok := mdm.cache[uid]; ok {
			continue
		}
		break
	}
	di := newMockDI(uid, data)
	mdm.cache[uid] = di
	return uid, nil
}

func (mdm *mockDM) Close() {
}
