package pindex_test

import (
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/dm/pindex"
	"testing"
)

func TestPindex(t *testing.T) {
	pindex := pindex.NewPindex()
	threshold := pcacher.PAGE_SIZE / 20
	for i := 0; i < 20; i++ {
		pindex.Add(pcacher.Pgno(i), i*threshold)
		pindex.Add(pcacher.Pgno(i), i*threshold)
		pindex.Add(pcacher.Pgno(i), i*threshold)
	}

	for k := 0; k < 3; k++ {
		for i := 0; i < 19; i++ {
			pgno, _, ok := pindex.Select(i * threshold)
			if ok != true {
				t.Fatal("error")
			}
			if int(pgno) != i+1 {
				t.Fatal("error")
			}
		}
	}

	_, _, ok := pindex.Select(19 * threshold)
	if ok != false {
		t.Fatal("error")
	}
}
