package tm

import "nyadb2/utils"

type XID utils.UUID

const (
	LEN_XID   = utils.LEN_UUID
	SUPER_XID = 0
)

func PutXID(buf []byte, xid XID) {
	utils.PutUUID(buf, utils.UUID(xid))
}

func ParseXID(raw []byte) XID {
	return XID(utils.ParseUUID(raw))
}

func XIDToRaw(xid XID) []byte {
	return utils.UUIDToRaw(utils.UUID(xid))
}
