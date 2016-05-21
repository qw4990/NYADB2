/*
   transactionManager 中定义了transactionManager， 用于管理xid文件。
   每个事务都会有一个xid， xid文件中纪录了该事务当前的状态。
   每个事务在某时刻有三种状态：
       0. active       未结束
       1. committed    已经被提交
       2. aborted      已经被撤销

   xid文件中为每个事务指定了1byte的空间用于存储其状态。
   某事务byte的位移为(xid - 1) + XID_FILE_HEADER_SIZE。
   其中xid － 1是因为事务xid从1开始标号。
   XID_FILE_HEADER_SIZE为存储在xid文件其实位置的， 表示该文件元信息的字节长度。

   XID_FILE_HEADER的字段如下：
   位移   长度      类型          字段名                 描述
   0     LEN_XID   XID         fileXidCounter        该xid文件之前已经包含的xid数目
*/
package tm

import (
	"errors"
	"os"
	"sync"
)

const (
	_XID_FILE_HEADER_SIZE = LEN_XID // xid文件头长度
	_XID_FIELD_SIZE       = 1       // 每个事务在xid文件中使用字节长度

	_FIELD_TRAN_ACTIVE   = 0 // 事务三种状态
	_FIELD_TRAN_COMMITED = 1
	_FIELD_TRAN_ABORTED  = 2

	SUFFIX_XID = ".xid"
)

var (
	ErrBadXIDFile = errors.New("Bad XID File.")
)

type TransactionManager interface {
	Begin() XID
	Commit(xid XID)
	Abort(xid XID)
	IsActive(xid XID) bool
	IsCommited(xid XID) bool
	IsAborted(xid XID) bool
	Close()
}

type transactionManager struct {
	file *os.File

	xidCounter  XID
	counterLock sync.Mutex
}

func Create(path string) *transactionManager {
	file, err := os.OpenFile(path+SUFFIX_XID, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

	xidCounterInit := make([]byte, LEN_XID)
	_, err = file.WriteAt(xidCounterInit, 0)
	if err != nil {
		panic(err)
	}

	return newTransactionManager(file)
}

func Open(path string) *transactionManager {
	file, err := os.OpenFile(path+SUFFIX_XID, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	return newTransactionManager(file)
}

func newTransactionManager(file *os.File) *transactionManager {
	tm := new(transactionManager)
	tm.file = file
	tm.checkXIDCounter()
	return tm
}

// checkXIDFile 检查该xid文件是否合法
// 合法性检查非常简单， 读取XID_FILE_HEADER中的xidcounter， 并根据它计算
// 文件长度， 再对比实际的文件长度。
func (tm *transactionManager) checkXIDCounter() {
	stat, err := tm.file.Stat()
	if err != nil {
		panic(err)
	}
	if stat.Size() < _XID_FILE_HEADER_SIZE {
		panic(ErrBadXIDFile)
	}

	tmp := make([]byte, _XID_FILE_HEADER_SIZE)
	_, err = tm.file.ReadAt(tmp, 0)
	if err != nil {
		panic(err)
	}
	tm.xidCounter = ParseXID(tmp)

	end, _ := xidPosition(XID(tm.xidCounter + 1))

	if end != stat.Size() {
		panic(ErrBadXIDFile)
	}
}

// xidPosition 根据事务xid取得其在xid文件中对应的位置
func xidPosition(xid XID) (int64, int) {
	offset := _XID_FILE_HEADER_SIZE + (xid-1)*_XID_FIELD_SIZE
	return int64(offset), _XID_FIELD_SIZE
}

// updateXID 更新某个事务的状态
func (t *transactionManager) updateXID(xid XID, status byte) {
	offset, length := xidPosition(xid)
	tmp := make([]byte, length)
	tmp[0] = byte(status)
	_, err := t.file.WriteAt(tmp, offset)
	if err != nil {
		panic(err)
	}
	err = t.file.Sync()
	if err != nil {
		panic(err)
	}
}

// incXIDCounter 将xid加1, 并更新xid的header部分
func (t *transactionManager) incXIDCounter() {
	t.xidCounter++

	buf := XIDToRaw(t.xidCounter)
	_, err := t.file.WriteAt(buf, 0)
	if err != nil {
		panic(err)
	}
	err = t.file.Sync()
	if err != nil {
		panic(err)
	}
}

// Begin 开始一个事务， 并返回xid作为handle
func (t *transactionManager) Begin() XID {
	t.counterLock.Lock()
	defer t.counterLock.Unlock()

	xid := t.xidCounter + 1
	t.updateXID(xid, _FIELD_TRAN_ACTIVE)
	t.incXIDCounter()
	return xid
}

// Commit 将xid这个事务提交
func (t *transactionManager) Commit(xid XID) {
	t.updateXID(xid, byte(_FIELD_TRAN_COMMITED))
}

// Abort 将xid这个事务回滚
func (t *transactionManager) Abort(xid XID) {
	t.updateXID(xid, byte(_FIELD_TRAN_ABORTED))
}

// checkTran 监测xid这个事务是否处于status状态
func (t *transactionManager) checkXID(xid XID, status byte) bool {
	offset, length := xidPosition(xid)
	tmp := make([]byte, length)
	_, err := t.file.ReadAt(tmp, offset)
	if err != nil {
		panic(err)
	}

	return tmp[0] == status
}
func (t *transactionManager) IsActive(xid XID) bool {
	if xid == SUPER_XID {
		return false
	}
	return t.checkXID(xid, _FIELD_TRAN_ACTIVE)
}
func (t *transactionManager) IsCommited(xid XID) bool {
	if xid == SUPER_XID {
		return true
	}
	return t.checkXID(xid, _FIELD_TRAN_COMMITED)
}
func (t *transactionManager) IsAborted(xid XID) bool {
	if xid == SUPER_XID {
		return false
	}
	return t.checkXID(xid, _FIELD_TRAN_ABORTED)
}

func (t *transactionManager) Close() {
	err := t.file.Close()
	if err != nil {
		panic(err)
	}
}
