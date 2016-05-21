/*
   mockLogger do nothing for all operations.
*/
package logger

type mockLogger struct {
}

func OpenMock(path string) *mockLogger   { return new(mockLogger) }
func CreateMock(path string) *mockLogger { return new(mockLogger) }

func (ml *mockLogger) Log(data []byte)        {}
func (ml *mockLogger) Truncate(x int64) error { return nil }
func (ml *mockLogger) Next() ([]byte, bool)   { return nil, true }
func (ml *mockLogger) Rewind()                {}
func (ml *mockLogger) Close()                 {}
