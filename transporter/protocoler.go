/*
	Protocoler 负责了包到二进制数据之间的转换.

	包的二进制格式编码如下:
	[flag] 1byte
	[data] *

	如果flag为0, 则表示要发送的是数据. 那么data既为这份数据本身.
	如果flag为1, 则表示要发送的是错误. 那么data为[]byte(err.Errors()).
*/
package transporter

import "errors"

var (
	ErrInvalidPkgData = errors.New("Invalid package data.")
)

type Protocoler interface {
	Encode(pkg Package) []byte
	Decode(data []byte) (Package, error)
}

type protocoler struct{}

func NewProtocoler() Protocoler {
	return new(protocoler)
}

func (p *protocoler) Decode(data []byte) (Package, error) {
	if len(data) < 1 {
		return nil, ErrInvalidPkgData
	}
	if data[0] == byte(0) { // 接受的是数据
		return NewPackage(data[1:], nil), nil
	} else if data[0] == byte(1) { // 接受的是错误
		err := errors.New(string(data[1:]))
		return NewPackage(nil, err), nil
	} else {
		return nil, ErrInvalidPkgData
	}
}

func (p *protocoler) Encode(pkg Package) []byte {
	if pkg.Err() != nil { // 发送的是错误
		err := pkg.Err()
		tmp := make([]byte, len(err.Error())+1)
		tmp[0] = byte(1)
		copy(tmp[1:], []byte(err.Error()))
		return tmp
	} else { // 发送的是数据
		tmp := make([]byte, len(pkg.Data())+1)
		tmp[0] = byte(0)
		copy(tmp[1:], pkg.Data())
		return tmp
	}
}
