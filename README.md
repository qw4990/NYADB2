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


##NYADB2运行说明
整个DB分为客户端和服务端.

####服务端
服务端启动代码为backend/launcher/launcher.go

服务端启动有create和open两种模式, create既新建一份数据库, open为打开指定路径的数据库. 请参考/backend/launcher下的open.sh和create.sh

除此之外, launcher还提供了一个mem参数, 用于指定数据库内存大小, 单位为"KB", "MB", "GB". 于是, 你可以在指定内存下打开你的数据库:

go run launcher.go -open="/tmp/nyadb" -mem="128MB"

服务端的启动地址默认为"localhost:8080", 目前没有提供接口用于修改启动地址, 不过你可以在launcher.go的源码内修改.

####客户端
客户端放在client/launcher.go

启动服务端后, 直接启动它即可进入客户端的shell.

更多信息请参考 http://www.codeyj.com/demo.html


##关于NYADB和NYADB2
NYADB和NYADB2的模型是一样的, NYADB2是在同样模型的基础上, 对NYADB进行的重构, 代码更加简洁.

NYADB作为我实验性的作品, 已经被kill掉了.

所以, 上面的NYADB模型文档, 和NYADB2的代码, 是不冲突的.

最后, 感谢一直给予我帮助的左老师, 感谢我大学四年的失败与努力.
