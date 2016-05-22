/*
   page为Pcacher于DM之间的协议, 有两条.

   Page共享协议:
       利用Page.Data()取得的数据, 是以内存共享的方式获得的.

   Page更新协议:
       在对Page做任何的更新之前, 一定需要吸纳调用Dirty().

   Page释放协议:
       在对Page操作完之后, 一定要调用Release()释放掉该页.
*/
package protocols
