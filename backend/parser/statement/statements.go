package statement

type Begin struct {
	IsRepeatableRead bool
}

type Commit struct{}
type Abort struct{}

type Drop struct {
	TableName string
}

type Show struct {
}

type Create struct {
	TableName string
	FieldName []string
	FieldType []string
	Index     []string
}

type Update struct {
	TableName string
	FieldName string
	Value     string
	Where     *Where
}

type Delete struct {
	TableName string
	Where     *Where
}

type Insert struct {
	TableName string
	Values    []string
}

type Read struct {
	TableName string
	Fields    []string
	Where     *Where
}

type Where struct {
	SingleExp1 *SingleExp
	LogicOp    string
	SingleExp2 *SingleExp
}

type SingleExp struct {
	Field string
	CmpOp string
	Value string
}
