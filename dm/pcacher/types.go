package pcacher

import "nyadb2/utils"

type Pgno uint32

func Pgno2UUID(pgno Pgno) utils.UUID {
	return utils.UUID(pgno)
}
func UUID2Pgno(uuid utils.UUID) Pgno {
	return Pgno(uuid)
}

type Offset uint16
