/*
   transporter 实现了client和server之间的通信.

   其结构大致为
    [Package]
       |  ^
       V  |
   [Packager] <---> [Protocoler]
        |
        v
    [Transporter]

    用户将要发送的数据打成包Pacakge, 然后传递给Packager, Packager利用Protocoler, 将包
    转化为二进制数据, 然后利用Transporter, 将二进制数据发送到另一端.

    另一端接受到二进制数据后, 采用和上面相反的过程, 解析出包内容.
*/
package transporter
