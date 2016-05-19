/*
   mockLogger do nothing for all operations.
*/
package logger

type mockLogger struct {
}

func OpenMockLogFile(path string) *mockLogger   { return new(mockLogger) }
func CreateMockLogFile(path string) *mockLogger { return new(mockLogger) }

func (ml *mockLogger) Log(data []byte)        {}
func (ml *mockLogger) Truncate(x int64) error { return nil }
func (ml *mockLogger) Next() ([]byte, bool)   { return nil, true }
func (ml *mockLogger) Rewind()                {}
func (ml *mockLogger) Close()                 {}
