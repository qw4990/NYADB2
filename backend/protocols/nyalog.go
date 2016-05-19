/*
   nyalog.go 描述了NYADB使用的日志协议, 该协议被使用于dm中, 用于保证NYADB的可恢复性.
   ***该协议需要上层模块提供一些保证.***

   dm实际上为上层模块提供了三种操作, Read, Insert, Update.
   Read不需要日志, 下面将重点描述Insert和Update日志, 以及怎么用它们来进行恢复.

   1. Insert日志格式:
   [Log Type] [XID] [Pgno] [Offset] [Raw]
   表示XID将Raw的内容插入到了Pgno页的Offset位移处.

   2. Updata日志格式:
   [Log Type] [XID] [UUID] [OldRaw] [NewRaw]
   表示XID将UUID这个dataitem从OldRaw更新为了NewRaw.

   3. 需要上层模块的保证:
    3.1)Update中, len(NewRaw) <= len(OldRaw)
    3.2)对dataitem的访问, 必须是可串行化的.

   利用日志进行恢复:
    对于所有非active的事务, 进行redo.
    对所有active的事务, 进行undo.

   事务redo:
    顺序的, 将该事务执行的所有Insert和Update操作再次执行一次.

   事务undo:
    倒序的, 将该事务的所有Insert和Update操作给undo掉.
    Update的undo: 将UUID恢复为OldRaw.
    Insert的undo: 先redo该条Insert, 然后将该UUID对应的DataItem设置为unvalid.(见dataitem.go)



   该文档只描述做法, 不论证正确性.
   正确性的论证去参考:https://qw4990.gitbooks.io/nyadb/content/
*/
package protocols
