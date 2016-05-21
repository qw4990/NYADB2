/*
	node.go 维护了B+树节点的内部结构
*/
package im

import (
	"nyadb2/backend/dm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
)

const (
	_IS_LEAF_OFFSET   = 0
	_NO_KEYS_OFFSET   = _IS_LEAF_OFFSET + 1
	_SIBLING_OFFSET   = _NO_KEYS_OFFSET + 2
	_NODE_HEADER_SIZE = _SIBLING_OFFSET + utils.LEN_UUID

	_BALANCE_NUMBER = 32
	_NODE_SIZE      = _NODE_HEADER_SIZE + (2*utils.LEN_UUID)*(_BALANCE_NUMBER*2+2)
)

/*
	node的二进制结构如下:
	[Leaf Flag] bool
	[No Of keys] uint16
	[Sibling UUID] UUID
	[Pair1], [Pair2] ... [PariN]
    [Son0] [Key0] [Son1] [Key1] ... [SonN] [KeyN]

	在一般的B+树算法中, 内部节点都会有一个MaxPointer, 指向最右边的子节点.
	我们这里将其特殊处理, 将MaxPointer处理成了SonN, 将keyN固定为INF.
	这样, 内部节点和叶节点就有了一致的二进制结构.
*/
type node struct {
	bt       *bPlusTree
	dataitem dm.Dataitem

	raw      []byte
	selfUUID utils.UUID
}

func setRawIsLeaf(raw []byte, isLeaf bool) {
	if isLeaf {
		raw[_IS_LEAF_OFFSET] = byte(1)
	} else {
		raw[_IS_LEAF_OFFSET] = byte(0)
	}
}

func getRawIsLeaf(raw []byte) bool {
	return raw[_IS_LEAF_OFFSET] == byte(1)
}

func setRawNoKeys(raw []byte, noKeys int) {
	utils.PutUint16(raw[_NO_KEYS_OFFSET:], uint16(noKeys))
}

func getRawNoKeys(raw []byte) int {
	return int(utils.ParseUint16(raw[_NO_KEYS_OFFSET:]))
}

func setRawSibling(raw []byte, sibling utils.UUID) {
	utils.PutUUID(raw[_SIBLING_OFFSET:], sibling)
}

func getRawSibling(raw []byte) utils.UUID {
	return utils.ParseUUID(raw[_SIBLING_OFFSET:])
}

func setRawKthSon(raw []byte, uid utils.UUID, kth int) {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2)
	utils.PutUUID(raw[offset:], uid)
}

func getRawKthSon(raw []byte, kth int) utils.UUID {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2)
	return utils.ParseUUID(raw[offset:])
}

func setRawKthKey(raw []byte, key utils.UUID, kth int) {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2) + utils.LEN_UUID
	utils.PutUUID(raw[offset:], key)
}

func getRawKthKey(raw []byte, kth int) utils.UUID {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2) + utils.LEN_UUID
	return utils.ParseUUID(raw[offset:])
}

func copyRawFromKth(from, to []byte, kth int) {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2)
	copy(to[_NODE_HEADER_SIZE:], from[offset:])
}

func shiftRawKth(raw []byte, kth int) {
	begin := _NODE_HEADER_SIZE + (kth+1)*(utils.LEN_UUID*2)
	end := _NODE_SIZE - 1
	for i := end; i >= begin; i-- { // copy(raw, raw) is dangerous
		raw[i] = raw[i-(utils.LEN_UUID*2)]
	}
}

// newRootRaw 新建一个根节点, 该根节点的初始两个子节点为left和right, 初始键值为key
func newRootRaw(left, right, key utils.UUID) []byte {
	raw := make([]byte, _NODE_SIZE)
	setRawIsLeaf(raw, false)
	setRawNoKeys(raw, 2)
	setRawSibling(raw, utils.NilUUID)
	setRawKthSon(raw, left, 0)
	setRawKthKey(raw, key, 0)
	setRawKthSon(raw, right, 1)
	setRawKthKey(raw, utils.INF, 1)
	return raw
}

// newNilRootRaw 新建一个空的根节点, 返回其二进制内容.
func newNilRootRaw() []byte {
	raw := make([]byte, _NODE_SIZE)
	setRawIsLeaf(raw, true)
	setRawNoKeys(raw, 0)
	setRawSibling(raw, utils.NilUUID)
	return raw
}

// loadNode 读入一个节点, 其自身地址为selfuuid
func loadNode(bt *bPlusTree, selfUUID utils.UUID) (*node, error) {
	dataitem, ok, err := bt.DM.Read(selfUUID)
	if err != nil {
		return nil, err
	}
	utils.Assert(ok == true)

	return &node{
		bt:       bt,
		dataitem: dataitem,
		raw:      dataitem.Data(),
		selfUUID: selfUUID,
	}, nil
}

func (u *node) Release() {
	u.dataitem.Release()
}

func (u *node) IsLeaf() bool {
	u.dataitem.RLock()
	defer u.dataitem.RUnlock()

	return getRawIsLeaf(u.raw)
}

// SearchNext 寻找对应key的uuid, 如果找不到, 则返回sibling uuid
func (u *node) SearchNext(key utils.UUID) (utils.UUID, utils.UUID) {
	u.dataitem.RLock()
	defer u.dataitem.RUnlock()

	noKeys := getRawNoKeys(u.raw)
	for i := 0; i < noKeys; i++ {
		ik := getRawKthKey(u.raw, i)
		if key < ik {
			return getRawKthSon(u.raw, i), utils.NilUUID
		}
	}
	return utils.NilUUID, getRawSibling(u.raw)
}

// LeafSearchRange 在该节点上查询属于[leftKey, rightKey]的地址,
// 如果rightKey大于等于该节点的最大的key, 则还返回一个sibling uuid.
func (u *node) LeafSearchRange(leftKey, rightKey utils.UUID) ([]utils.UUID, utils.UUID) {
	u.dataitem.RLock()
	defer u.dataitem.RUnlock()

	noKeys := getRawNoKeys(u.raw)
	var kth int
	for kth < noKeys {
		ik := getRawKthKey(u.raw, kth)
		if ik >= leftKey {
			break
		}
		kth++
	}

	var uuids []utils.UUID
	for kth < noKeys {
		ik := getRawKthKey(u.raw, kth)
		if ik <= rightKey {
			uuids = append(uuids, getRawKthSon(u.raw, kth))
			kth++
		} else {
			break
		}
	}

	var sibling utils.UUID = utils.NilUUID
	if kth == noKeys {
		sibling = getRawSibling(u.raw)
	}

	return uuids, sibling
}

/*
		      p, k         p', k'
				 |         |
 			 	 v         v
	p0, k0, p1, k1         p2, k2, p3, INF
*/
// InsertAndSplit 将对应的数据插入该节点, 并尝试进行分裂.
// 如果该份数据不应该插入到此节点, 则返回一个sibling uuid.
func (u *node) InsertAndSplit(uuid, key utils.UUID) (utils.UUID, utils.UUID, utils.UUID, error) {
	var succ bool
	var err error

	u.dataitem.Before()
	defer func() {
		if err == nil && succ {
			u.dataitem.After(tm.SUPER_XID)
		} else { // 如果失败, 则复原当前节点
			u.dataitem.UnBefore()
		}
	}()

	succ = u.insert(uuid, key)
	if succ == false {
		return getRawSibling(u.raw), utils.NilUUID, utils.NilUUID, nil
	}

	if u.needSplit() {
		var newSon utils.UUID
		var newKey utils.UUID
		newSon, newKey, err = u.split()
		return utils.NilUUID, newSon, newKey, err
	} else {
		return utils.NilUUID, utils.NilUUID, utils.NilUUID, nil
	}
}

func (u *node) insert(uuid utils.UUID, key utils.UUID) bool {
	noKeys := getRawNoKeys(u.raw)
	var kth int
	for kth < noKeys {
		ik := getRawKthKey(u.raw, kth)
		if ik < key {
			kth++
		} else {
			break
		}
	}
	if kth == noKeys && getRawSibling(u.raw) != utils.NilUUID {
		// 如果该节点有右继节点, 且该key大于该节点所有key
		// 则让该key被插入到右继节点去
		return false
	}

	if getRawIsLeaf(u.raw) == true {
		shiftRawKth(u.raw, kth)
		setRawKthKey(u.raw, key, kth)
		setRawKthSon(u.raw, uuid, kth)
		setRawNoKeys(u.raw, noKeys+1)
	} else {
		kk := getRawKthKey(u.raw, kth)
		setRawKthKey(u.raw, key, kth)
		shiftRawKth(u.raw, kth+1)
		setRawKthKey(u.raw, kk, kth+1)
		setRawKthSon(u.raw, uuid, kth+1)
		setRawNoKeys(u.raw, noKeys+1)
	}
	return true
}

func (u *node) needSplit() bool {
	return _BALANCE_NUMBER*2 == getRawNoKeys(u.raw)
}

func (u *node) split() (utils.UUID, utils.UUID, error) {
	nodeRaw := make([]byte, _NODE_SIZE)

	setRawIsLeaf(nodeRaw, getRawIsLeaf(u.raw))
	setRawNoKeys(nodeRaw, _BALANCE_NUMBER)
	setRawSibling(nodeRaw, getRawSibling(u.raw))
	copyRawFromKth(u.raw, nodeRaw, _BALANCE_NUMBER)
	son, err := u.bt.DM.Insert(tm.SUPER_XID, nodeRaw)

	if err != nil {
		return utils.NilUUID, utils.NilUUID, err
	}

	setRawNoKeys(u.raw, _BALANCE_NUMBER)
	setRawSibling(u.raw, son)

	return son, getRawKthKey(nodeRaw, 0), nil
}
