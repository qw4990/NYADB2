package tm

type MockTranManager struct {
}

func CreateMockXIDFile(path string) *MockTranManager {
	return new(MockTranManager)
}
func OpenMockXIDFile(path string) *MockTranManager {
	return new(MockTranManager)
}

func (mtm *MockTranManager) Begin() XID {
	return 0
}

func (mtm *MockTranManager) Commit(xid XID) {
}
func (mtm *MockTranManager) Abort(xid XID) {
}
func (mtm *MockTranManager) IsActive(xid XID) bool {
	return false
}
func (mtm *MockTranManager) IsCommited(xid XID) bool {
	return false
}
func (mtm *MockTranManager) IsAborted(xid XID) bool {
	return false
}
func (mtm *MockTranManager) Close() {
}
