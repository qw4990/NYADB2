package tbm

import (
	"nyadb2/backend/parser/statement"
	"nyadb2/backend/tm"
)

type TableManager interface {
	Begin(begin *statement.Begin) (tm.XID, []byte, error)
	Commit(xid tm.XID) ([]byte, error)
	Abort(xid tm.XID) []byte

	Create(xid tm.XID, create *statement.Create) ([]byte, error)
	Show(xid tm.XID) []byte
	Insert(xid tm.XID, insert *statement.Insert) ([]byte, error)
	Read(xid tm.XID, read *statement.Read) ([]byte, error)
	Update(xid tm.XID, update *statement.Update) ([]byte, error)
	Delete(xid tm.XID, delete *statement.Delete) ([]byte, error)
}
