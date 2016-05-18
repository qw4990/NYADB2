package dm

import (
	"nyadb2/dm/pcacher"
	"nyadb2/tm"
	"nyadb2/utils/cacher"
)

type DataManager interface {
	Read(handle Handle) (Dataitem, bool, error)
	Insert(xid tm.XID, data []byte) (Handle, error)
	BootDataItem() (Dataitem, error)

	Close()
}

type dataManager struct {
	xx tm.TransactionManager
	pc pcacher.Pcacher
	c  cacher.Cacher
}
