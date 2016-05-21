package tbm

import (
	"errors"
	"nyadb2/backend/dm"
	"nyadb2/backend/parser/statement"
	"nyadb2/backend/sm"
	"nyadb2/backend/tbm/booter"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
)

var (
	ErrDuplicatedTable = errors.New("Duplicated table.")
	ErrNoThatTable     = errors.New("No that table.")
)

type TableManager interface {
	Begin(begin *statement.Begin) (tm.XID, []byte)
	Commit(xid tm.XID) ([]byte, error)
	Abort(xid tm.XID) []byte

	Show(xid tm.XID) []byte
	Create(xid tm.XID, create *statement.Create) ([]byte, error)

	Insert(xid tm.XID, insert *statement.Insert) ([]byte, error)
	Read(xid tm.XID, read *statement.Read) ([]byte, error)
	// Update(xid tm.XID, update *statement.Update) ([]byte, error)
	// Delete(xid tm.XID, delete *statement.Delete) ([]byte, error)
}

type tableManager struct {
	DM dm.DataManager
	SM sm.SerializabilityManager

	booter booter.Booter

	tc   map[string]*table   // 表缓存
	xtc  map[tm.XID][]*table // xid 创建了哪些表
	lock sync.Mutex
}

func newTableManager(sm sm.SerializabilityManager, dm dm.DataManager, booter booter.Booter) *tableManager {
	tbm := &tableManager{
		DM:     dm,
		SM:     sm,
		booter: booter,
		tc:     make(map[string]*table),
		xtc:    make(map[tm.XID][]*table),
	}

	tbm.loadTables()
	return tbm
}

func Create(path string, sm sm.SerializabilityManager, dm dm.DataManager) *tableManager {
	booter := booter.Create(path)
	booter.Update(utils.UUIDToRaw(utils.NilUUID))
	return newTableManager(sm, dm, booter)
}

func Open(path string, sm sm.SerializabilityManager, dm dm.DataManager) *tableManager {
	booter := booter.Open(path)
	return newTableManager(sm, dm, booter)
}

// loadTables 将所有的table读入内存.
func (tbm *tableManager) loadTables() {
	uuid := tbm.firstTableUUID()
	for uuid != utils.NilUUID {
		tb := LoadTable(tbm, uuid)
		uuid = tb.Next
		tbm.tc[tb.Name] = tb
	}
}

func (tbm *tableManager) firstTableUUID() utils.UUID {
	raw := tbm.booter.Load()
	return utils.ParseUUID(raw)
}

func (tbm *tableManager) updateFirstTableUUID(uuid utils.UUID) {
	raw := utils.UUIDToRaw(uuid)
	tbm.booter.Update(raw)
}

func (tbm *tableManager) Read(xid tm.XID, read *statement.Read) ([]byte, error) {
	tbm.lock.Lock()
	tb, ok := tbm.tc[read.TableName]
	tbm.lock.Unlock()
	if ok == false {
		return nil, ErrNoThatTable
	}

	result, err := tb.Read(xid, read)
	if err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (tbm *tableManager) Insert(xid tm.XID, insert *statement.Insert) ([]byte, error) {
	tbm.lock.Lock()
	tb, ok := tbm.tc[insert.TableName]
	tbm.lock.Unlock()
	if ok == false {
		return nil, ErrNoThatTable
	}

	err := tb.Insert(xid, insert)
	if err != nil {
		return nil, err
	}
	return []byte("Insert"), nil
}

func (tbm *tableManager) Create(xid tm.XID, create *statement.Create) ([]byte, error) {
	tbm.lock.Lock()
	defer tbm.lock.Unlock()

	_, ok := tbm.tc[create.TableName]
	if ok == true { // 已经存在
		return nil, ErrDuplicatedTable
	}

	// 直接创建新表
	tb, err := CreateTable(tbm, tbm.firstTableUUID(), xid, create)
	if err != nil {
		return nil, err
	} else { // 创建成功
		tbm.updateFirstTableUUID(tb.SelfUUID)
		tbm.tc[create.TableName] = tb
		tbm.xtc[xid] = append(tbm.xtc[xid], tb)
		return []byte("create " + create.TableName), nil
	}
}

/*
	Show 返回所有的表名.
*/
func (tbm *tableManager) Show(xid tm.XID) []byte {
	tbm.lock.Lock()
	defer tbm.lock.Unlock()
	var results []byte
	for _, t := range tbm.tc { // 打印已经提交的表
		tPrint := t.Print()
		results = append(results, tPrint...)
		results = append(results, '\n')
	}

	for _, t := range tbm.xtc[xid] { // 打印它自己创建的表
		tPrint := t.Print()
		results = append(results, tPrint...)
		results = append(results, '\n')
	}

	return results
}

func (tbm *tableManager) Begin(begin *statement.Begin) (tm.XID, []byte) {
	var level int
	if begin.IsRepeatableRead {
		level = 1
	}
	xid := tbm.SM.Begin(level)
	return xid, []byte("begin")
}

func (tbm *tableManager) Commit(xid tm.XID) ([]byte, error) {
	err := tbm.SM.Commit(xid)
	if err != nil {
		return nil, err
	}
	return []byte("commit"), nil
}

func (tbm *tableManager) Abort(xid tm.XID) []byte {
	tbm.SM.Abort(xid)
	return []byte("abort")
}
