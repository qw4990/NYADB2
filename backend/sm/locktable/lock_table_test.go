package locktable_test

import (
	"nyadb2/backend/sm/locktable"
	"nyadb2/backend/utils"
	"testing"
)

func TestLockTable(t *testing.T) {
	lt := locktable.NewLockTable()
	ok, _ := lt.Add(1, 1)
	if ok == false {
		t.Fatal("Error")
	}
	ok, _ = lt.Add(2, 2)
	if ok == false {
		t.Fatal("Error")
	}
	ok, _ = lt.Add(2, 1)
	if ok == false {
		t.Fatal("Error")
	}
	ok, _ = lt.Add(1, 2)
	if ok == true {
		t.Fatal("Error")
	}
}

func TestLockTable2(t *testing.T) {
	lt := locktable.NewLockTable()
	for i := 1; i <= 100; i++ {
		ok, ch := lt.Add(utils.UUID(i), utils.UUID(i))
		if ok == false {
			t.Fatal("Error")
		}
		go func() {
			<-ch
		}()
	}
	for i := 1; i <= 99; i++ {
		ok, ch := lt.Add(utils.UUID(i), utils.UUID(i+1))
		if ok == false {
			t.Fatal("Error")
		}
		go func() {
			<-ch
		}()
	}

	ok, _ := lt.Add(100, 1)
	if ok == true {
		t.Fatal("Error")
	}

	lt.Remove(23)
	ok, _ = lt.Add(100, 1)
	if ok == false {
		t.Fatal("Error")
	}
}
