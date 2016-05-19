package pcacher

import "nyadb2/backend/utils"

type Pgno uint32

const (
	LEN_PGNO = 4
)

func Pgno2UUID(pgno Pgno) utils.UUID {
	return utils.UUID(pgno)
}
func UUID2Pgno(uuid utils.UUID) Pgno {
	return Pgno(uuid)
}
func PutPgno(buf []byte, pgno Pgno) {
	utils.PutUint32(buf, uint32(pgno))
}
func ParsePgno(raw []byte) Pgno {
	return Pgno(utils.ParseUint32(raw))
}
