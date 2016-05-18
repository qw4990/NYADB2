package tm

type MockTranManager struct {
}

func NewMockTranManager() *MockTranManager {
	return new(MockTranManager)
}

func (mtm *MockTranManager) Begin() (XID, error) {
	return 0, nil
}

func (mtm *MockTranManager) Commit(xid XID) error {
	return nil
}
func (mtm *MockTranManager) Abort(xid XID) error {
	return nil
}
func (mtm *MockTranManager) IsActive(xid XID) (bool, error) {
	return false, nil
}
func (mtm *MockTranManager) IsCommited(xid XID) (bool, error) {
	return false, nil
}
func (mtm *MockTranManager) IsAborted(xid XID) (bool, error) {
	return false, nil
}
func (mtm *MockTranManager) Close() error {
	return nil
}
