/*
	page1.go 实现了对page1的特殊管理.

	目前对page1的特殊用途有:

	ValidCheck:
		[ValidCheck, ValidCheck+8), [ValidCheck+8, ValidCheck+16)
		这两段区间, 被用于检测数据库的正确性.
		方法为, 在每次启动时, 生成一个随机序列, 并打入第一个区间, 在Close时,
		将第一个区间的内容复制到第二个区间.
		这样, 在每次重启时, 都检验两个区间内的值是否一致, 如果不一致, 则说明上次为正常结束,
		则对数据库进行恢复.
*/
package dm

import (
	"bytes"
	"nyadb2/backend/dm/pcacher"
	"nyadb2/backend/utils"
)

const (
	_P1_OF_VC  = 100 // valid check
	_P1_LEN_VC = 8
)

// P1InitRaw 返回page1初始内容
func P1InitRaw() []byte {
	raw := make([]byte, pcacher.PAGE_SIZE)
	p1RawSetVCOpen(raw)
	return raw
}

// p1RawSetVCOpen 为VC1填入一串随机数列.
func p1RawSetVCOpen(raw []byte) {
	copy(raw[_P1_OF_VC:_P1_OF_VC+_P1_LEN_VC], utils.RandBytes(_P1_LEN_VC))
}

// p1RawSetVCClose 将VC1的内容拷贝到VC2
func p1RawSetVCClose(raw []byte) {
	copy(raw[_P1_OF_VC+_P1_LEN_VC:_P1_OF_VC+_P1_LEN_VC*2], raw[_P1_OF_VC:_P1_OF_VC+_P1_LEN_VC])
}

// P1SetVCOpen 让dm在Open的时候调用.
func P1SetVCOpen(pg pcacher.Page) {
	pg.Dirty()
	p1RawSetVCOpen(pg.Data())
}

// P1SetVCClose 让dm在Close的时候调用.
func P1SetVCClose(pg pcacher.Page) {
	pg.Dirty()
	p1RawSetVCClose(pg.Data())
}

// P1CheckVC 对page1进行VC检验.
func P1CheckVC(pg pcacher.Page) bool {
	return p1RawCheckVC(pg.Data())
}
func p1RawCheckVC(raw []byte) bool {
	return bytes.Compare(raw[_P1_OF_VC:_P1_OF_VC+_P1_LEN_VC], raw[_P1_OF_VC+_P1_LEN_VC:_P1_OF_VC+_P1_LEN_VC*2]) == 0
}
