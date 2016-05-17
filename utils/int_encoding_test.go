package utils

import "testing"

func TestUint32(t *testing.T) {
	var i uint32
	for ; i < 23333; i++ {
		tmp := Uint32ToRaw(i)
		if i != ParseUint32(tmp) {
			t.Fatal(" Error")
		}
	}
}

func TestInt64(t *testing.T) {
	tmp := Int64ToRaw(2333)
	if 2333 != ParseInt64(tmp) {
		t.Fatal(" Error")
	}

	tmp = Int64ToRaw(0)
	if 0 != ParseInt64(tmp) {
		t.Fatal(" Error")
	}

	tmp = Int64ToRaw(-2333)
	if -2333 != ParseInt64(tmp) {
		t.Fatal(" Error")
	}
}
