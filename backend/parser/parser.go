/*
	parser.go 实现了对语句的解析
	语法见syntax.txt
*/
package parser

import (
	"errors"

	"nyadb2/backend/parser/statement"
)

var (
	ErrInvalidStat = errors.New("Invalid command.")
	ErrHasNoIndex  = errors.New("Table has no index.")
)

func Parse(statement []byte) (interface{}, error) {
	tokener := newTokener(statement)
	token, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	var stat interface{}
	var staterr error

	switch token {
	case "begin":
		stat, staterr = parseBegin(tokener)
	case "commit":
		stat, staterr = parseCommit(tokener)
	case "abort":
		stat, staterr = parseAbort(tokener)
	case "create":
		stat, staterr = parseCreate(tokener)
	case "drop":
		stat, staterr = parseDrop(tokener)
	case "read":
		stat, staterr = parseRead(tokener)
	case "insert":
		stat, staterr = parseInsert(tokener)
	case "delete":
		stat, staterr = parseDelete(tokener)
	case "update":
		stat, staterr = parseUpdate(tokener)
	case "show":
		stat, staterr = parseShow(tokener)
	default:
		return nil, ErrInvalidStat
	}

	next, err := tokener.Peek()
	if err == nil && next != "" {
		errStat := tokener.errStat()
		staterr = errors.New("Invalid Stat: " + string(errStat))
	}

	return stat, staterr
}

func parseShow(tokener *tokener) (*statement.Show, error) {
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		return new(statement.Show), nil
	} else {
		return nil, ErrInvalidStat
	}
}

func parseUpdate(tokener *tokener) (*statement.Update, error) {
	var err error
	update := new(statement.Update)
	update.TableName, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	set, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if set != "set" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	update.FieldName, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "=" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	update.Value, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	tmp, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" { // no where statement
		update.Where = nil
		return update, nil
	}

	where, err := parseWhere(tokener) // parse where statement
	if err != nil {
		return nil, err
	}
	update.Where = where
	return update, nil
}

func parseDelete(tokener *tokener) (*statement.Delete, error) {
	deleteStat := new(statement.Delete)

	from, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if from != "from" {
		return nil, ErrInvalidStat
	}

	tokener.Pop()
	tableName, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	deleteStat.TableName = tableName

	tokener.Pop()
	where, err := parseWhere(tokener)
	if err != nil {
		return nil, err
	}
	deleteStat.Where = where
	return deleteStat, nil
}

func parseInsert(tokener *tokener) (*statement.Insert, error) {
	insert := new(statement.Insert)

	into, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if into != "into" {
		return nil, ErrInvalidStat
	}

	tokener.Pop()
	tableName, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	insert.TableName = tableName

	tokener.Pop()
	values, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if values != "values" {
		return nil, ErrInvalidStat
	}

	for { // get value list
		tokener.Pop()
		value, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if value == "" { // eof
			break
		} else {
			insert.Values = append(insert.Values, value)
		}
	}

	return insert, nil
}

func parseRead(tokener *tokener) (*statement.Read, error) {
	read := new(statement.Read)

	asterisk, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if asterisk == "*" {
		read.Fields = append(read.Fields, "*")
		tokener.Pop()
	} else {
		for { // parse field to be read
			field, err := tokener.Peek()
			if err != nil {
				return nil, err
			}
			if isName(field) == false {
				return nil, ErrInvalidStat
			}
			read.Fields = append(read.Fields, field)

			tokener.Pop()
			comma, err := tokener.Peek()
			if err != nil {
				return nil, err
			}
			if comma == "," { // has more fields
				tokener.Pop() // pop ","
			} else {
				break
			}
		}
	}

	from, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if from != "from" {
		return nil, ErrInvalidStat
	}

	tokener.Pop()                    // pop from
	tableName, err := tokener.Peek() // get table name
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	read.TableName = tableName

	tokener.Pop() // pop table name
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" { // no where statement
		read.Where = nil
		return read, nil
	}

	where, err := parseWhere(tokener) // parse where statement
	if err != nil {
		return nil, err
	}
	read.Where = where
	return read, nil
}

func parseWhere(tokener *tokener) (*statement.Where, error) {
	where := new(statement.Where)

	whereStr, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if whereStr != "where" {
		return nil, ErrInvalidStat
	}
	tokener.Pop() // pop where

	sexp1, err := parseSingleExpr(tokener)
	if err != nil {
		return nil, err
	}
	where.SingleExp1 = sexp1

	logicOp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if logicOp == "" { // eof, only one compare statement
		where.LogicOp = ""
		return where, nil
	}
	if isLogicOp(logicOp) == false {
		return nil, ErrInvalidStat
	}
	where.LogicOp = logicOp
	tokener.Pop() // pop logicop

	sexp2, err := parseSingleExpr(tokener)
	if err != nil {
		return nil, err
	}
	where.SingleExp2 = sexp2

	eof, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if eof != "" {
		return nil, ErrInvalidStat
	}

	return where, nil
}

func parseSingleExpr(tokener *tokener) (*statement.SingleExp, error) {
	singleExp := new(statement.SingleExp)

	field, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(field) == false {
		return nil, ErrInvalidStat
	}
	singleExp.Field = field
	tokener.Pop()

	op, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isCmpOp(op) == false {
		return nil, ErrInvalidStat
	}
	singleExp.CmpOp = op
	tokener.Pop()

	value, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	singleExp.Value = value
	tokener.Pop()

	return singleExp, nil
}

func parseDrop(tokener *tokener) (*statement.Drop, error) {
	table, err := tokener.Peek() // get table
	if err != nil {
		return nil, err
	}
	if table != "table" {
		return nil, ErrInvalidStat
	}

	tokener.Pop()
	tableName, err := tokener.Peek() // get table name
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}

	tokener.Pop()
	eof, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if eof != "" {
		return nil, ErrInvalidStat
	}

	drop := new(statement.Drop)
	drop.TableName = tableName
	return drop, nil
}

func parseCreate(tokener *tokener) (*statement.Create, error) {
	table, err := tokener.Peek() // get table
	if err != nil {
		return nil, err
	}
	if table != "table" {
		return nil, ErrInvalidStat
	}

	create := new(statement.Create)
	tokener.Pop()               // pop table
	name, err := tokener.Peek() // get table name
	if err != nil {
		return nil, err
	}
	if isName(name) == false {
		return nil, ErrInvalidStat
	}
	create.TableName = name

	for { // get field and type
		tokener.Pop()
		field, err := tokener.Peek() // get field

		if field == "(" { // has index
			break
		}

		if err != nil {
			return nil, err
		}
		if isName(field) == false {
			return nil, ErrInvalidStat
		}

		tokener.Pop()                // pop field
		ftype, err := tokener.Peek() // get field type
		if err != nil {
			return nil, err
		}
		if isType(ftype) == false {
			return nil, ErrInvalidStat
		}

		create.FieldName = append(create.FieldName, field)
		create.FieldType = append(create.FieldType, ftype)

		tokener.Pop() // pop field type
		next, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if next == "," { // has next field
		} else if next == "" { // is eof, return now
			return nil, ErrHasNoIndex
		} else if next == "(" { // has index
			break
		} else { // error statement
			return nil, ErrInvalidStat
		}
	}

	// get index
	tokener.Pop()                // pop '('
	index, err := tokener.Peek() // get index
	if err != nil {
		return nil, err
	}
	if index != "index" {
		return nil, ErrInvalidStat
	}
	for { // get all fields to be indexed
		tokener.Pop()
		field, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if field == ")" {
			break
		} else if isName(field) == false {
			return nil, ErrInvalidStat
		} else {
			create.Index = append(create.Index, field)
		}
	}
	tokener.Pop() // pop ')'
	eof, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if eof != "" {
		return nil, ErrInvalidStat
	}
	return create, nil
}

func isLogicOp(op string) bool {
	return op == "and" || op == "or"
}

func isType(tp string) bool {
	return tp == "int32" || tp == "int64" ||
		tp == "float" || tp == "string"
}

func isName(name string) bool {
	return !(len(name) == 1 && isAlphaBeta(name[0]) == false)
}

func isCmpOp(op string) bool {
	return op == "=" || op == ">" || op == "<"
}

func parseBegin(tokener *tokener) (*statement.Begin, error) {
	isolation, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	begin := new(statement.Begin)
	if isolation == "" {
		return begin, nil
	}
	if isolation != "isolation" {
		return nil, ErrInvalidStat
	}

	tokener.Pop()
	level, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if level != "level" {
		return nil, ErrInvalidStat
	}

	tokener.Pop()
	tmp1, err := tokener.Peek()
	if err != nil {
		return nil, err
	}

	if tmp1 == "read" {
		tokener.Pop()
		tmp2, err := tokener.Peek()
		if tmp2 == "committed" {
			tokener.Pop()
			eof, err := tokener.Peek()
			if err != nil {
				return nil, err
			}
			if eof != "" {
				return nil, ErrInvalidStat
			}

			return begin, nil
		} else {
			return nil, err
		}
	} else if tmp1 == "repeatable" {
		tokener.Pop()
		tmp2, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if tmp2 == "read" {
			begin.IsRepeatableRead = true
			tokener.Pop()
			eof, err := tokener.Peek()
			if err != nil {
				return nil, err
			}
			if eof != "" {
				return nil, ErrInvalidStat
			}
			return begin, nil
		} else {
			return nil, ErrInvalidStat
		}
	} else {
		return nil, ErrInvalidStat
	}
}

func parseCommit(tokener *tokener) (*statement.Commit, error) {
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		return new(statement.Commit), nil
	} else {
		return nil, ErrInvalidStat
	}
}

func parseAbort(tokener *tokener) (*statement.Abort, error) {
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		return new(statement.Abort), nil
	} else {
		return nil, ErrInvalidStat
	}
}
