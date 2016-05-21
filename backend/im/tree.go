/*
	tree.go 和 node.go实现了B+树算法.
	该B+树基于DM, 且支持并发, 无死锁.

	B+树算法, 以及其并发协议, 请参考文档内容.
*/

package im

import (
	"nyadb2/backend/dm"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
	"sync"
)

type BPlusTree interface {
	Insert(key, uuid utils.UUID) error
	Search(key utils.UUID) ([]utils.UUID, error)
	SearchRange(leftKey, rightKey utils.UUID) ([]utils.UUID, error)
}

/*
	每棵B+树都有一个bootUUID, 可通过它向DM读取该树的boot.
	B+树boot里面存储了B+树根节点的地址.

	PS: 因为B+树在算法执行过程中, 根节点可能会发生改变, 所以不能直接用根节点的地址当boot,
	而需要一个固定的boot, 用来指向它的根节点.

	PS: 目前B+树支持的最大键值为INF-1
*/
type bPlusTree struct {
	bootUUID     utils.UUID
	bootDataitem dm.Dataitem
	bootLock     sync.Mutex

	DM dm.DataManager
}

// CreateBPlusTree 创建一棵B+树, 并返回其bootUUID.
func Create(dm dm.DataManager) (utils.UUID, error) {
	rawRoot := newNilRootRaw()
	rootUUID, err := dm.Insert(tm.SUPER_XID, rawRoot)
	if err != nil {
		return utils.NilUUID, err
	}
	bootUUID, err := dm.Insert(tm.SUPER_XID, utils.UUIDToRaw(rootUUID))
	if err != nil {
		return utils.NilUUID, err
	}

	return bootUUID, nil
}

// LoadBPlusTree 通过BootUUID读取一课B+树, 并返回它.
func Load(bootUUID utils.UUID, dm dm.DataManager) (BPlusTree, error) {
	bootDataitem, ok, err := dm.Read(bootUUID)
	if err != nil {
		return nil, err
	}
	utils.Assert(ok == true)

	return &bPlusTree{
		bootUUID:     bootUUID,
		DM:           dm,
		bootDataitem: bootDataitem,
	}, nil
}

// rootUUID 通过bootUUID读取该树的根节点地址
func (bt *bPlusTree) rootUUID() utils.UUID {
	bt.bootLock.Lock()
	defer bt.bootLock.Unlock()
	return utils.ParseUUID(bt.bootDataitem.Data())
}

// updaterootUUID 更新该树的根节点
func (bt *bPlusTree) updateRootUUID(left, right, rightKey utils.UUID) error {
	bt.bootLock.Lock()
	defer bt.bootLock.Unlock()

	rootRaw := newRootRaw(left, right, rightKey)
	newRootUUID, err := bt.DM.Insert(tm.SUPER_XID, rootRaw)
	if err != nil {
		return err
	}

	bt.bootDataitem.Before()
	copy(bt.bootDataitem.Data(), utils.UUIDToRaw(newRootUUID))
	bt.bootDataitem.After(tm.SUPER_XID)
	return nil
}

// searchLeaf 根据key, 在nodeUUID代表节点的子树中搜索, 直到找到其对应的叶节点地址.
func (bt *bPlusTree) searchLeaf(nodeUUID, key utils.UUID) (utils.UUID, error) {
	node, err := loadNode(bt, nodeUUID)
	if err != nil {
		return utils.NilUUID, err
	}

	isLeaf := node.IsLeaf()
	node.Release()

	if isLeaf {
		return nodeUUID, nil
	} else {
		next, err := bt.searchNext(nodeUUID, key)
		if err != nil {
			return utils.NilUUID, err
		}
		return bt.searchLeaf(next, key)
	}
}

// serachNext 从nodeUUID对应节点开始, 不断的向右试探兄弟节点, 找到对应key的next uuid
func (bt *bPlusTree) searchNext(nodeUUID, key utils.UUID) (utils.UUID, error) {
	for {
		node, err := loadNode(bt, nodeUUID)
		if err != nil {
			return utils.NilUUID, err
		}
		next, siblingUUID := node.SearchNext(key)
		node.Release()
		if next != utils.NilUUID {
			return next, nil
		}
		nodeUUID = siblingUUID
	}
}

func (bt *bPlusTree) Search(key utils.UUID) ([]utils.UUID, error) {
	return bt.SearchRange(key, key)
}

func (bt *bPlusTree) SearchRange(leftKey, rightKey utils.UUID) ([]utils.UUID, error) {
	rootUUID := bt.rootUUID()

	leafUUID, err := bt.searchLeaf(rootUUID, leftKey)
	if err != nil {
		return nil, err
	}

	var uuids []utils.UUID
	for { // 不断的从leaf向sibling迭代, 将所有满足的uuid都加入
		leaf, err := loadNode(bt, leafUUID)
		if err != nil {
			return nil, err
		}
		tmp, siblingUUID := leaf.LeafSearchRange(leftKey, rightKey)
		leaf.Release()
		uuids = append(uuids, tmp...)

		if siblingUUID == utils.NilUUID {
			break
		} else {
			leafUUID = siblingUUID
		}
	}

	return uuids, nil
}

// Insert 向B+树种插入(uuid, key)的键值对
func (bt *bPlusTree) Insert(key, uuid utils.UUID) error {
	rootUUID := bt.rootUUID()

	newNode, newKey, err := bt.insert(rootUUID, uuid, key)
	if err != nil {
		return err
	}

	/*
		如果newNode != nil, 则需要更变根节点了.

		// TODO
		这里有一个小bug, 如果同时有多个事务都准备updaterootUUID，
		那么会相互覆盖.
		但是由于我们的_BALANCE_NUMBER比较大, 故一般不会出现这种情况,
		暂且先作为一个未处理bug.
	*/
	if newNode != utils.NilUUID { // 更新根节点
		err := bt.updateRootUUID(rootUUID, newNode, newKey)
		if err != nil {
			return err
		}
	}

	return nil
}

// insert 将(uuid, key)插入到B+树中, 如果有分裂, 则将分裂产生的新节点也返回.
func (bt *bPlusTree) insert(nodeUUID, uuid, key utils.UUID) (newNodeUUID, newNodeKey utils.UUID, err error) {
	var node *node
	node, err = loadNode(bt, nodeUUID)
	if err != nil {
		return
	}

	isLeaf := node.IsLeaf()
	node.Release()

	if isLeaf {
		newNodeUUID, newNodeKey, err = bt.insertAndSplit(nodeUUID, uuid, key)
	} else {
		var next utils.UUID
		next, err = bt.searchNext(nodeUUID, key)
		if err != nil {
			return
		}

		var newSonUUId utils.UUID
		var newSonKey utils.UUID
		newSonUUId, newSonKey, err = bt.insert(next, uuid, key)
		if err != nil {
			return
		}

		if newSonUUId != utils.NilUUID { // split
			newNodeUUID, newNodeKey, err = bt.insertAndSplit(nodeUUID, newSonUUId, newSonKey)
		}
	}
	return
}

// insertAndSplit 函数从node开始, 不断的向右试探兄弟节点, 直到找到一个节点, 能够插入进对应的值
func (bt *bPlusTree) insertAndSplit(nodeUUID, uuid, key utils.UUID) (utils.UUID, utils.UUID, error) {
	for {
		node, err := loadNode(bt, nodeUUID)
		if err != nil {
			return utils.NilUUID, utils.NilUUID, err
		}
		siblingSon, newNodeSon, newNodeKey, err := node.InsertAndSplit(uuid, key)
		node.Release()

		if siblingSon != utils.NilUUID { // 继续向sibling尝试
			nodeUUID = siblingSon
		} else {
			return newNodeSon, newNodeKey, err
		}
	}

}

func (bt *bPlusTree) Close() {
	bt.bootDataitem.Release()
}
