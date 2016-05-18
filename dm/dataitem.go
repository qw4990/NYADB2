package dm

import "nyadb2/tm"

/*
   Dataitem 为DataEngine为上层模块提供的数据抽象
   上层模块需要根据地址， 向DataEngine请求对应的Dataitem
   然后通过Data方法， 取得DataItem实际内容

   下面是一些关于DataItem的协议.

 	数据共享:
		利用d.Data()得到的数据, 是内存共享的.

  	数据项修改协议:
   		上层模块在对数据项进行任何修改之前, 都必须调用d.Before(), 如果想撤销修改, 则再调用
		d.UnBefore(). 修改完成后, 还必须调用d.After(xid).

	数据项释放协议:
		上层模块不用数据项时, 必须调用d.Release()来将其释放
*/
type Dataitem interface {
	Data() []byte   // Data 以共享形式返回该dataitem的数据内容
	Handle() Handle // Handle 返回该dataitem的handle

	Before()
	UnBefore()
	After(xid tm.XID)
	Release()

	// 下面是DM为上层模块提供的针对DataItem的锁操作.
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

type Handle interface {
	ToRaw() []byte
}
