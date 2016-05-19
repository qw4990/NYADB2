package utils

type UUID uint64

const (
	LEN_UUID = 8
)

func PutUUID(buf []byte, uid UUID) {
	PutUint64(buf, uint64(uid))
}

func ParseUUID(raw []byte) UUID {
	return UUID(ParseUint64(raw))
}

func UUIDToRaw(uid UUID) []byte {
	return Uint64ToRaw(uint64(uid))
}
