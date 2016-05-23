package utils_test

import (
	"nyadb2/backend/utils"
	"testing"
)

func TestStrEncoding(t *testing.T) {
	if utils.Uint32ToStr(2333) != "2333" {
		t.Fatal("error")
	}
	if utils.Int64ToStr(233333333) != "233333333" {
		t.Fatal("error")
	}

	i32, err := utils.StrToUint32("233")
	if i32 != 233 {
		t.Fatal(err)
	}
	i64, err := utils.StrToInt64("233")
	if i64 != 233 {
		t.Fatal(err)
	}
}
