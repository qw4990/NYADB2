package sm

import "nyadb2/backend/tm"

type transaction struct {
	XID      tm.XID
	Level    int             // 隔离度
	snapshot map[tm.XID]bool // 快照
	Err      error           // 发生的错误， 该事务只能被回滚
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
	_, ok := t.snapshot[xid]
	return ok
}
