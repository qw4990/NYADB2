package dm

import (
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/utils"
)

func UUID2Address(uid utils.UUID) (pcacher.Pgno, Offset) {
	u := uint64(uid)
	offset := Offset(u & ((1 << 16) - 1))
	u >>= 32
	pgno := pcacher.Pgno(u & ((1 << 32) - 1))
	return pgno, offset
}

func Address2UUID(pgno pcacher.Pgno, offset Offset) utils.UUID {
	u0 := uint64(pgno)
	u1 := uint64(offset)
	return utils.UUID((u0 << 32) | u1)
}

type Offset uint16

const LEN_OFFSET = 4

func PutOffset(buf []byte, offset Offset) {
	utils.PutUint16(buf, uint16(offset))
}

func ParseOffset(raw []byte) Offset {
	return Offset(utils.ParseUint16(raw))
}

func OffsetToRaw(offset Offset) []byte {
	return utils.Uint16ToRaw(uint16(offset))
}
