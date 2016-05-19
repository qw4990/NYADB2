package utils

func StrToUint64(str string) uint64 {
	var result uint64
	for _, n := range str {
		result = result*10 + uint64(n-'0')
	}
	return result
}

func Uint64ToStr(num uint64) string {
	if num == 0 {
		return "0"
	}

	buf := make([]byte, 30)
	i := 0
	for num > 0 {
		t := num % 10
		num /= 10
		buf[i] = byte(t + '0')
		i++
	}
	for k := 0; k < i/2; k++ {
		buf[k], buf[i-k-1] = buf[i-k-1], buf[k]
	}
	return string(buf[:i])
}
