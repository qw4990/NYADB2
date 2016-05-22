/*
	transaction.go 实现了sm内部的transaction结构, 该结构内保存了sm中事务需要的必要的信息.
*/
package sm

import "nyadb2/backend/tm"

type transaction struct {
	XID          tm.XID
	Level        int             // 隔离度
	snapshot     map[tm.XID]bool // 快照
	Err          error           // 发生的错误， 该事务只能被回滚
	AutoAbortted bool            // 该事务是否被自动回滚
}

func newTransaction(xid tm.XID, level int, active map[tm.XID]*transaction) *transaction {
	t := &transaction{
		XID:      xid,
		Level:    level,
		snapshot: nil,
	}
	if level != 0 {
		t.snapshot = make(map[tm.XID]bool)
		for xid, _ := range active {
			t.snapshot[xid] = true
		}
	}
	return t
}

func (t *transaction) InSnapShot(xid tm.XID) bool {
	if xid == tm.SUPER_XID { // 忽略SUPER_XID
		return false
	}
	_, ok := t.snapshot[xid]
	return ok
}
