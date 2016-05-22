/*
   dataitem.go 描述了使用Dataitem的模块应该遵守的协议.
   目前, 使用Dataitem的模块有SM和IM.

   数据共享:
        利用d.Data()得到的数据, 是内存共享的.

   数据项修改协议:
       上层模块在对数据项进行任何修改之前, 都必须调用d.Before(), 如果想撤销修改, 则再调用
   d.UnBefore(). 修改完成后, 还必须调用d.After(xid).
   DM会保证对Dataitem的修改是原子性的.

   数据项释放协议:
        上层模块不用数据项时, 必须调用d.Release()来将其释放
*/
package protocols
