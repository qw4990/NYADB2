package booter_test

import (
	"bytes"
	"nyadb2/backend/tbm/booter"
	"testing"
)

func TestBooter(t *testing.T) {
	bt := booter.Create("/tmp/booter_test")
	raw := []byte("123456jksadhfjksadflkwejflk;n")

	bt.Update(raw)

	if bytes.Compare(raw, bt.Load()) != 0 {
		t.Fatal(raw, " ", bt.Load())
	}
}
