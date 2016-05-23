/*
	field.go 管理具体的字段.

	一个field的二进制格式为
	[Field Name]   string
	[Type Name]    string
	[Index UUID]   UUID

	如果该field没有索引, 那么[Index UUID]为NilUUID.
*/
package tbm

import (
	"errors"
	"nyadb2/backend/im"
	"nyadb2/backend/parser/statement"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
)

var (
	ErrInvalidFieldType  = errors.New("Invalid field type.")
	ErrInvalidFieldValue = errors.New("Invalid field value.")
)

type field struct {
	SelfUUID utils.UUID
	tb       *table

	FName string
	FType string
	index utils.UUID
	bt    im.BPlusTree
}

/*
	LoadField 从DB中读入field的内容.
	panic的原因和LoadTable类似.
*/
func LoadField(tb *table, uuid utils.UUID) *field {
	raw, ok, err := tb.TBM.SM.Read(tm.SUPER_XID, uuid)
	utils.Assert(ok)
	if err != nil {
		panic(err)
	}
	f := &field{
		SelfUUID: uuid,
		tb:       tb,
	}
	f.parseSelf(raw)
	return f
}

func (f *field) parseSelf(raw []byte) {
	var pos, shift int
	f.FName, shift = utils.ParseVarStr(raw[pos:])
	pos += shift
	f.FType, shift = utils.ParseVarStr(raw[pos:])
	pos += shift
	f.index = utils.ParseUUID(raw[pos:])
	if f.index != utils.NilUUID {
		var err error
		f.bt, err = im.Load(f.index, f.tb.TBM.DM)
		if err != nil {
			panic(err)
		}
	}
}

func CreateField(tb *table, xid tm.XID, fname, ftype string, indexed bool) (*field, error) {
	err := typeCheck(ftype)
	if err != nil {
		return nil, err
	}

	f := &field{
		tb:    tb,
		FName: fname,
		FType: ftype,
		index: utils.NilUUID,
	}

	if indexed {
		index, err := im.Create(tb.TBM.DM)
		if err != nil {
			return nil, err
		}
		bt, err := im.Load(index, tb.TBM.DM)
		if err != nil {
			return nil, err
		}
		f.index = index
		f.bt = bt
	}

	err = f.persistSelf(xid)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// persist 将该field持久化
func (f *field) persistSelf(xid tm.XID) error {
	raw := utils.VarStrToRaw(f.FName)
	raw = append(raw, utils.VarStrToRaw(f.FType)...)
	raw = append(raw, utils.UUIDToRaw(f.index)...)
	self, err := f.tb.TBM.SM.Insert(xid, raw)
	if err != nil {
		return err
	}
	f.SelfUUID = self
	return nil
}

func typeCheck(ftype string) error {
	if ftype != "uint32" && ftype != "uint64" && ftype != "string" {
		return ErrInvalidFieldType
	}
	return nil
}

func (f *field) Print() string {
	str := "("
	str += f.FName
	str += ", " + f.FType
	if f.index != utils.NilUUID {
		str += ", Index"
	} else {
		str += ", NoIndex"
	}
	str += ")"
	return str
}

func (f *field) IsIndexed() bool {
	return f.index != utils.NilUUID
}

// Insert 将(key, uuid)这键值对插入到该field的索引中
func (f *field) Insert(key interface{}, uuid utils.UUID) error {
	ukey := f.ValueToUUID(key)
	return f.bt.Insert(ukey, uuid)
}

func (f *field) Search(left, right utils.UUID) ([]utils.UUID, error) {
	return f.bt.SearchRange(left, right)
}

func (f *field) StrToValue(valStr string) (interface{}, error) {
	var v interface{}
	var err error
	switch f.FType {
	case "uint32":
		v, err = utils.StrToUint32(valStr)
	case "uint64":
		v, err = utils.StrToUint64(valStr)
	case "string":
		v = valStr
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (f *field) ValueToRaw(v interface{}) []byte {
	var raw []byte
	switch f.FType {
	case "uint32":
		raw = utils.Uint32ToRaw(v.(uint32))
	case "uint64":
		raw = utils.Uint64ToRaw(v.(uint64))
	case "string": // 转换为VarStr
		raw = utils.VarStrToRaw(v.(string))
	}
	return raw
}

func (f *field) ParseValue(raw []byte) (interface{}, int) {
	var v interface{}
	var shift int
	switch f.FType {
	case "uint32":
		v = utils.ParseUint32(raw)
		shift = 4
	case "uint64":
		v = utils.ParseUint64(raw)
		shift = 8
	case "string": // 解析出VarStr
		v, shift = utils.ParseVarStr(raw)
	}
	return v, shift
}

func (f *field) ValueToUUID(v interface{}) utils.UUID {
	var uuid utils.UUID
	switch f.FType {
	case "uint32":
		uuid = utils.UUID(v.(uint32))
	case "uint64":
		uuid = utils.UUID(v.(uint64))
	case "string": // 解析出VarStr
		uuid = utils.StrToUUID(v.(string))
	}
	return uuid
}

func (f *field) ValuePrint(v interface{}) string {
	var str string
	switch f.FType {
	case "uint32":
		str = utils.Uint32ToStr(v.(uint32))
	case "uint64":
		str = utils.Uint64ToStr(v.(uint64))
	case "string": // 解析出VarStr
		str = v.(string)
	}
	return str
}

func (f *field) CalExp(exp *statement.SingleExp) (left, right utils.UUID, err error) {
	var v interface{}
	if exp.CmpOp == "<" {
		left = 0
		v, err = f.StrToValue(exp.Value)
		if err != nil {
			return
		}
		right = f.ValueToUUID(v) + 1
	} else if exp.CmpOp == "=" {
		v, err = f.StrToValue(exp.Value)
		if err != nil {
			return
		}
		left = f.ValueToUUID(v)
		right = left
	} else if exp.CmpOp == ">" {
		right = utils.INF
		v, err = f.StrToValue(exp.Value)
		if err != nil {
			return
		}
		left = f.ValueToUUID(v) + 1
	}
	return
}
