# 推荐项目
最近看了go版boltDB的源码, 强烈推荐此项目;

boltDB为一个单机的KV数据库, 模型异常简单且漂亮, 代码简洁, 总共代码(除去测试代码)只有6K左右;

boltDB已经被ETCD3.0+采用; 

最近我应该会整理一份boltDB模型和实现的文档, 请期待;

NYADB的模型比boltDB复杂不少, 主要是为了提供更多的机制;

对于想学习数据库模型的同学, 强烈推荐阅读boltDB源码, 理解boltDB模型, 阅读其代码, 不只是学习, 也是享受;

boltDB分析: https://github.com/qw4990/blog/tree/master/database/boltDB

# 未来计划

+ 更加模块化NYADB: 将NYADB的组件继续模块化, 使用更加通用的组件和概念, 使NYADB更加简单, 易学;
+ 分布式: 引入raft协议模块, 使其可以组合成为一个分布式数据库;
+ 强化前端: 自定义一套简单的SQL, 利用YACC工具来强化前端部分;
+ 引入中间层(调研中): 参照SQLITE引入一个类似于"虚拟机"的中间层, 做到前后端分离, 调研中...
+ 完善英文文档.

由于工作比较忙, 故上述任务的完成时间不确定, 预留的底线是2017年年底之前完成.

目标是将NYADB做成一份易学习, 模块化程度高, 模型简单, 运行稳定的分布式数据库; 期望大部分人都能够看得懂, 并能够产生美的体验.

如果你有好的想法, 能够帮助简化NYADB, 请通过我的邮箱qw4990@163.com, 或者直接提Issue告诉我, 我会非常感谢.

祝大家生活开心:P

# NYADB2
NYADB2是一个Go编写的数据库.

它主要实现了:
+ 数据库的可靠性
+ 事务处理
+ MVCC
+ 并发索引

我创建NYADB2的目的是为了学习, NYADB2的模型简单, 实现简洁, 整个代码+注释大概约7000行.

毫无疑问, NYADB2也还有许多并不完善的地方, 我将会持续的完善她, 以后将会有NYADB3, 4, 5, ...

<b>NYADB的模型</b>: https://qw4990.gitbooks.io/nyadb/content/

NYADB演示: http://www.codeyj.com/nyadb/demo.html

NYADB简单的效率测试: http://www.codeyj.com/nyadb/performance.html

NYADB文法: https://qw4990.gitbooks.io/nyadb/content/chapter5.html

NYADB主页: http://www.codeyj.com, http://www.nyadb.org

NYADB2一些实现细节: 待续

NYADB2最终结构: 
![Alt text](https://github.com/qw4990/NYADB2/blob/master/arch.png)


## NYADB2运行说明
整个DB分为客户端和服务端.

#### 服务端
服务端启动代码为backend/launcher/launcher.go

服务端启动有create和open两种模式, create既新建一份数据库, open为打开指定路径的数据库. 请参考/backend/launcher下的open.sh和create.sh

除此之外, launcher还提供了一个mem参数, 用于指定数据库内存大小, 单位为"KB", "MB", "GB". 于是, 你可以在指定内存下打开你的数据库:

go run launcher.go -open="/tmp/nyadb" -mem="128MB"

服务端的启动地址默认为"localhost:8080", 目前没有提供接口用于修改启动地址, 不过你可以在launcher.go的源码内修改.

#### 客户端
客户端放在client/launcher.go

启动服务端后, 直接启动它即可进入客户端的shell.

更多信息请参考 http://www.codeyj.com/demo.html


## 关于NYADB和NYADB2
NYADB和NYADB2的模型是一样的, NYADB2是在同样模型的基础上, 对NYADB进行的重构, 代码更加简洁.

NYADB作为我实验性的作品, 已经被kill掉了.

所以, 上面的NYADB模型文档, 和NYADB2的代码, 是不冲突的.

最后, 感谢一直给予我帮助的左老师, 感谢我大学四年的失败与努力.
