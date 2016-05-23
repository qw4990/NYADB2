package utils

import (
	"strconv"
)

// StrToUUID 将str转换为UUID, 使用了一种简单的hash算法, 可能会有冲突.
// 另外转换后的UUID将无序.
func StrToUUID(str string) UUID {
	var seed uint64 = 13331
	var result uint64
	for _, b := range str {
		result = result*seed + uint64(b)
	}
	return UUID(result)
}

func VarStrToRaw(str string) []byte {
	length := len(str)
	raw := Uint32ToRaw(uint32(length))
	raw = append(raw, []byte(str)...)
	return raw
}

func ParseVarStr(raw []byte) (string, int) {
	length := ParseUint32(raw)
	return string(raw[4 : 4+length]), int(length) + 4
}

func StrToUint64(str string) (uint64, error) {
	return strconv.ParseUint(str, 10, 64)
}

func Uint64ToStr(num uint64) string {
	return strconv.FormatUint(num, 10)
}

func StrToInt64(str string) (int64, error) {
	return strconv.ParseInt(str, 10, 64)
}

func Int64ToStr(num int64) string {
	return strconv.FormatInt(num, 10)
}

func StrToUint32(str string) (uint32, error) {
	i64, err := strconv.ParseUint(str, 10, 32)
	return uint32(i64), err
}

func Uint32ToStr(num uint32) string {
	return strconv.FormatUint(uint64(num), 10)
}
