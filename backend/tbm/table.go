/*
   table.go 维护了表的结构.
   表的二进制结构如下:
   	[Table Name]      string
   	[Next Table]      UUID
   	[Field1 UUID, Field2 UUID, ..., FieldN UUID]
*/
package tbm

import (
	"errors"
	"nyadb2/backend/parser/statement"
	"nyadb2/backend/tm"
	"nyadb2/backend/utils"
)

var (
	ErrInvalidValues   = errors.New("Invalid values.")
	ErrInvalidLogOP    = errors.New("Invalid logic operation.")
	ErrNoThatField     = errors.New("No that field.")
	ErrFieldHasNoField = errors.New("Field has no index.")
)

// map[Field]Value
type entry map[string]interface{}

type table struct {
	TBM      *tableManager
	SelfUUID utils.UUID

	Name   string
	status byte
	Next   utils.UUID
	fields []*field
}

/*
	LoadTable 从数据库中将uuid指定的table读入内存.
	该函数只会在TM启动时被调用.
	因为该函数被调用时, 为单线程, 所以不会有ErrCacheFull之类的错误, 因此一旦遇到错误, 那一定
	是不可恢复的错误, 应该直接panic.
*/
func LoadTable(tbm *tableManager, uuid utils.UUID) *table {
	raw, ok, err := tbm.SM.Read(tm.SUPER_XID, uuid)
	utils.Assert(ok)
	if err != nil {
		panic(err)
	}

	tb := &table{
		TBM:      tbm,
		SelfUUID: uuid,
	}

	tb.parseSelf(raw)
	return tb
}

// parseSelf 通过raw解析出table自己的信息.
func (t *table) parseSelf(raw []byte) {
	var pos, shift int
	t.Name, shift = utils.ParseVarStr(raw[pos:])
	pos += shift
	t.Next = utils.ParseUUID(raw[pos:])
	pos += utils.LEN_UUID

	for pos < len(raw) {
		uuid := utils.ParseUUID(raw[pos:])
		pos += utils.LEN_UUID
		f := LoadField(t, uuid)
		t.fields = append(t.fields, f)
	}
}

func CreateTable(tbm *tableManager, next utils.UUID, xid tm.XID, create *statement.Create) (*table, error) {
	tb := &table{
		TBM:  tbm,
		Name: create.TableName,
		Next: next,
	}

	for i := 0; i < len(create.FieldName); i++ {
		fname := create.FieldName[i]
		ftype := create.FieldType[i]
		indexed := false
		for j := 0; j < len(create.Index); j++ {
			if create.Index[j] == fname {
				indexed = true
				break
			}
		}
		field, err := CreateField(tb, xid, fname, ftype, indexed)
		if err != nil {
			return nil, err
		}
		tb.fields = append(tb.fields, field)
	}

	err := tb.persistSelf(xid)
	if err != nil {
		return nil, err
	}

	return tb, nil
}

// persist 将t自身持久化到磁盘上, 该函数只会在CreateTable的时候被调用
func (t *table) persistSelf(xid tm.XID) error {
	raw := utils.VarStrToRaw(t.Name)
	raw = append(raw, utils.UUIDToRaw(t.Next)...)
	for _, f := range t.fields {
		raw = append(raw, utils.UUIDToRaw(f.SelfUUID)...)
	}

	self, err := t.TBM.SM.Insert(xid, raw)
	if err != nil {
		return err
	}

	t.SelfUUID = self
	return nil
}

func (t *table) Print() string {
	str := "{"
	str += t.Name + ": "
	for i := 0; i < len(t.fields); i++ {
		str += t.fields[i].Print()
		if i == len(t.fields)-1 {
			str += "}"
		} else {
			str += ", "
		}
	}
	return str
}

func (t *table) Read(xid tm.XID, read *statement.Read) (string, error) {
	var l0, r0, l1, r1 utils.UUID
	single := false
	var err error
	var fd *field

	if read.Where == nil {
		for _, f := range t.fields {
			if f.IsIndexed() {
				fd = f
				break
			}
		}
		l0, r0 = 0, utils.INF
		single = true
	} else if read.Where != nil {
		for _, f := range t.fields {
			if f.FName == read.Where.SingleExp1.Field {
				if f.IsIndexed() == false {
					return "", ErrFieldHasNoField
				}
				fd = f
				break
			}
		}
		if fd == nil {
			return "", ErrNoThatField
		}

		l0, r0, l1, r1, single, err = t.calWhere(fd, read.Where)
		if err != nil {
			return "", err
		}
	}

	uuids, err := fd.Search(l0, r0)
	if err != nil {
		return "", err
	}
	if single == false {
		tmp, err := fd.Search(l1, r1)
		if err != nil {
			return "", err
		}
		uuids = append(uuids, tmp...)
	}

	result := ""
	for _, uuid := range uuids {
		raw, ok, err := t.TBM.SM.Read(xid, uuid)
		if err != nil {
			return "", err
		}
		if ok == false {
			continue
		}
		e := t.parseEntry(raw)
		result += t.entryPrint(e) + "\n"
	}

	return result, nil
}

func (t *table) calWhere(fd *field, where *statement.Where) (l0, r0, l1, r1 utils.UUID, single bool, err error) {
	if where.LogicOp == "" { // single
		single = true
		l0, r0, err = fd.CalExp(where.SingleExp1)
	} else if where.LogicOp == "or" {
		single = false
		l0, r0, err = fd.CalExp(where.SingleExp1)
		if err != nil {
			return
		}
		l1, r1, err = fd.CalExp(where.SingleExp2)
	} else if where.LogicOp == "and" {
		single = true
		l0, r0, err = fd.CalExp(where.SingleExp1)
		if err != nil {
			return
		}
		l1, r1, err = fd.CalExp(where.SingleExp2)
		// 合并[l0, r0], [l1, r1]两个区间
		if l1 > l0 {
			l0 = l1
		}
		if r1 < r0 {
			r0 = r1
		}
		return
	} else {
		err = ErrInvalidLogOP
	}
	return
}

// Insert 对该表执行insert语句.
func (t *table) Insert(xid tm.XID, insert *statement.Insert) error {
	e, err := t.strToEntry(insert.Values) // 将insert的values转换为entry
	if err != nil {
		return err
	}

	raw := t.entryToRaw(e) // 将该entry插入到DB
	uuid, err := t.TBM.SM.Insert(xid, raw)
	if err != nil {
		return err
	}

	for _, f := range t.fields { // 更新对应的索引
		if f.IsIndexed() {
			err := f.Insert(e[f.FName], uuid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *table) strToEntry(values []string) (entry, error) {
	if len(values) != len(t.fields) {
		return nil, ErrInvalidValues
	}

	e := entry{}
	for i, f := range t.fields {
		v, err := f.StrToValue(values[i])
		if err != nil {
			return nil, err
		}
		e[f.FName] = v
	}

	return e, nil
}

func (t *table) entryToRaw(e entry) []byte {
	var raw []byte
	for _, f := range t.fields {
		raw = append(raw, f.ValueToRaw(e[f.FName])...)
	}
	return raw
}

func (t *table) parseEntry(raw []byte) entry {
	var pos, shift int
	e := entry{}
	for _, f := range t.fields {
		e[f.FName], shift = f.ParseValue(raw[pos:])
		pos += shift
	}
	return e
}

func (t *table) entryPrint(e entry) string {
	str := "["
	for i, f := range t.fields {
		str += f.ValuePrint(e[f.FName])
		if i == len(t.fields)-1 {
			str += "]"
		} else {
			str += ", "
		}
	}
	return str
}
