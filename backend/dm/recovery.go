/*
   recover.go 对数据库进行恢复, 恢复的具体策略见protocols/recovery.go
*/
package dm

import (
	"nyadb2/backend/dm/logger"
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
)

const (
	_LOG_TYPE_INSERT = 0
	_LOG_TYPE_UPDATE = 1

	_REDO = 0
	_UNDO = 1
)

// recover 对数据库进行恢复.
func Recover(tm tm.TransactionManager, lg logger.Logger, pc pcacher.Pcacher) {
	utils.Info("Recovering...")
	defer utils.Info("Recovery Over.")
	/*
	   第一步: 找出之前最大的页号, 并将DB文件扩充到该页号的大小的空间.
	*/
	lg.Rewind()
	var maxPgno pcacher.Pgno
	for {
		log, ok := lg.Next()
		if ok == false {
			break
		}
		var pgno pcacher.Pgno
		if isInsertLog(log) {
			_, pgno, _, _ = parseInsertLog(log)
		} else {
			_, pgno, _, _, _ = parseUpdateLog(log)
		}
		if pgno > maxPgno {
			maxPgno = pgno
		}
	}
	if maxPgno == 0 { // 即使maxPgno为0, page1是能被DM保证在磁盘上的
		maxPgno = 1
	}
	pc.TruncateByPgno(maxPgno)
	utils.Info("Truncate to ", maxPgno, " pages.")

	/*
		第二步: redo所有非active的事务.
	*/
	redoTransactions(tm, lg, pc)
	utils.Info("Redo Transactions Over.")
	/*
		第三步: undo所有active的事务.
	*/
	undoTransactions(tm, lg, pc)
	utils.Info("Undo Transactions Over.")
}

// redoTransactions 将所有非active的事务redo.
func redoTransactions(tm tm.TransactionManager, lg logger.Logger, pc pcacher.Pcacher) {
	lg.Rewind()
	for {
		log, ok := lg.Next()
		if ok == false {
			break
		}
		if isInsertLog(log) {
			xid, _, _, _ := parseInsertLog(log)
			if tm.IsActive(xid) == false { // redo
				doInsertLog(pc, log, _REDO)
			}
		} else {
			xid, _, _, _, _ := parseUpdateLog(log)
			if tm.IsActive(xid) == false { // redo
				doUpdateLog(pc, log, _REDO)
			}
		}
	}
}

// undoTransactions 对所有的active事务进行undo
func undoTransactions(tm0 tm.TransactionManager, lg logger.Logger, pc pcacher.Pcacher) {
	//	第一步: 对所有active事务的log进行缓存, 以待倒序的undo它们.
	logCache := make(map[tm.XID][][]byte)
	lg.Rewind()
	for {
		log, ok := lg.Next()
		if ok == false {
			break
		}
		if isInsertLog(log) {
			xid, _, _, _ := parseInsertLog(log)
			if tm0.IsActive(xid) == true {
				logCache[xid] = append(logCache[xid], log)
			}
		} else {
			xid, _, _, _, _ := parseUpdateLog(log)
			if tm0.IsActive(xid) == true {
				logCache[xid] = append(logCache[xid], log)
			}
		}
	}

	//	第二步: 对所有active的log进行倒序的undo.
	for xid, logs := range logCache {
		for i := len(logs) - 1; i >= 0; i-- {
			log := logs[i]
			if isInsertLog(log) {
				doInsertLog(pc, log, _UNDO)
			} else {
				doUpdateLog(pc, log, _UNDO)
			}
		}
		tm0.Abort(xid) // 恢复完成后将该事务标记为Aborted.
	}
}

func isInsertLog(log []byte) bool {
	return log[0] == _LOG_TYPE_INSERT
}

/*
	[Log Type] [XID] [UUID] [OldRaw] [NewRaw]
	表示XID将UUID这个dataitem从OldRaw更新为了NewRaw.
*/
func UpdateLog(xid tm.XID, di *dataitem) []byte {
	log := make([]byte, 1+tm.LEN_XID+utils.LEN_UUID+len(di.raw)*2)
	pos := 0
	log[pos] = _LOG_TYPE_UPDATE
	pos++
	tm.PutXID(log[pos:], xid)
	pos += tm.LEN_XID
	utils.PutUUID(log[pos:], di.uid)
	pos += utils.LEN_UUID
	copy(log[pos:], di.oldraw)
	pos += len(di.oldraw)
	copy(log[pos:], di.raw)
	return log
}

func parseUpdateLog(log []byte) (tm.XID, pcacher.Pgno, Offset, []byte, []byte) {
	pos := 1
	xid := tm.ParseXID(log[pos:])
	pos += tm.LEN_XID
	uuid := utils.ParseUUID(log[pos:])
	pgno, offset := UUID2Address(uuid)
	pos += utils.LEN_UUID
	length := (len(log) - pos) / 2
	oldraw := log[pos : pos+length]
	newraw := log[pos+length : pos+length*2]
	return xid, pgno, offset, oldraw, newraw
}

func doUpdateLog(pc pcacher.Pcacher, log []byte, flag int) {
	var pgno pcacher.Pgno
	var offset Offset
	var raw []byte
	if flag == _REDO {
		_, pgno, offset, _, raw = parseUpdateLog(log)
	} else {
		_, pgno, offset, raw, _ = parseUpdateLog(log)
	}
	pg, err := pc.GetPage(pgno)
	if err != nil {
		// 因为在恢复的时候是单线程的, 所以err不可能为ErrCacheFull等并发错误,
		// 如果此时出错, 那么一定是不能解决的问题, 所以应该直接panic.
		panic(err)
	}
	defer pg.Release()
	PXRecoverUpdate(pg, offset, raw)
}

/*
   [Log Type] [XID] [Pgno] [Offset] [Raw]
   表示XID将Raw的内容插入到了Pgno页的Offset位移处.
*/
func InsertLog(xid tm.XID, pg pcacher.Page, raw []byte) []byte {
	log := make([]byte, 1+tm.LEN_XID+pcacher.LEN_PGNO+2+len(raw))
	pos := 0
	log[pos] = _LOG_TYPE_INSERT
	pos++
	tm.PutXID(log[pos:], xid)
	pos += tm.LEN_XID
	pcacher.PutPgno(log[pos:], pg.Pgno())
	pos += pcacher.LEN_PGNO
	PutOffset(log[pos:], PxFSO(pg))
	pos += LEN_OFFSET
	copy(log[pos:], raw)
	return log
}

func parseInsertLog(log []byte) (tm.XID, pcacher.Pgno, Offset, []byte) {
	pos := 1
	xid := tm.ParseXID(log[pos:])
	pos += tm.LEN_XID
	pgno := pcacher.ParsePgno(log[pos:])
	pos += pcacher.LEN_PGNO
	offset := ParseOffset(log[pos:])
	pos += LEN_OFFSET
	return xid, pgno, offset, log[pos:]
}

/*
	redoInsertLog 对insertLog进行redo.
	redo的方式为将原数据重新插入到对应page的位置, 然后将page的FSO设置为较大的那一个.

	TODO: WARN: 这里可能有个BUG.
	BUG出现在"将page的FSO设置为较大的那一个", 如果之前数据库刚好崩坏在对page的FSO做修改时,
	那么坏的FSO可能会非常大, 导致到最后恢复完成时, FSO保留的就是那个错误的大值.

	危害: 虽然FSO很大, 其实并没有什么危害. 只会导致该page的剩余空间难以被利用, 对之前已经插入
	该page的数据, 没有影响. 所以暂时不进行修复.
*/
func doInsertLog(pc pcacher.Pcacher, log []byte, flag int) {
	_, pgno, offset, raw := parseInsertLog(log)
	pg, err := pc.GetPage(pgno)
	if err != nil {
		panic(err) // 和上面同理
	}
	defer pg.Release()
	if flag == _UNDO { // 如果为UNDO, 则把该dataitem标记为非法.
		InValidRawDataitem(raw)
	}
	PXRecoverInsert(pg, offset, raw)
}
