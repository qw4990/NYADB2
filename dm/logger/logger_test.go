package logger

import "testing"

func TestLogger(t *testing.T) {
	lg := CreateLogFile("/tmp/logger_test.log")
	lg.Log([]byte("aaa"))
	lg.Log([]byte("bbb"))
	lg.Log([]byte("ccc"))
	lg.Log([]byte("ddd"))
	lg.Log([]byte("eee"))
	lg.Close()

	lg, err := OpenLogFile("/tmp/logger_test.log")
	if err != nil {
		t.Fatal(err)
	}
	lg.Rewind()

	log, end := lg.Next()
	if end != false {
		t.Fatal("error")
	}
	if string(log) != string("aaa") {
		t.Fatal("error")
	}

	log, end = lg.Next()
	if end != false {
		t.Fatal("error")
	}
	if string(log) != string("bbb") {
		t.Fatal("error")
	}

	log, end = lg.Next()
	if end != false {
		t.Fatal("error")
	}
	if string(log) != string("ccc") {
		t.Fatal("error")
	}

	log, end = lg.Next()
	if end != false {
		t.Fatal("error")
	}
	if string(log) != string("ddd") {
		t.Fatal("error")
	}

	log, end = lg.Next()
	if end != false {
		t.Fatal("error")
	}
	if string(log) != string("eee") {
		t.Fatal("error")
	}

	_, end = lg.Next()
	if end != true {
		t.Fatal("error")
	}
}
