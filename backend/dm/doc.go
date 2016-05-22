/*
   DM的理论模型请参考 https://qw4990.gitbooks.io/nyadb/content

   DM实现的模型为:
                [DM] <--抽象出--> [Dataitem]
        +---------+--------+
        |         |        |
        v         v        v
    [Pcacher] [Logger]  [Pindex]
        |        |
        v        v
  [DB File]   [Log File]

  DM对磁盘上的数据库文件进行分页管理, 并抽象出Dataitem的概念, 并提供了Dataitem上的Read,
  Update, Insert操作, 供上层模块使用, 使得上层模块不用关心具体的文件细节. 同时也有利于DM
  实现数据库的可恢复性.

  DM的日志策略, 请参考"模型"文档, 及protocols/recovery.go

  Pcacher实现了对磁盘文件分页的缓存.

  Logger实现了对日志文件操作的逻辑.

  Pindex管理的是(Pgno, FreeSpace)的键值对, 使得DM在执行插入操作时, 能够快速的选出合适大小
  的页, 将数据插入其中. Pindex全部被维护在内存中.
*/
package dm
