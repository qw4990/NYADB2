/*
   tokener 负责将语句给token化。
   使用自动机来完成。
   // TODO
   自动机状态图见http://nothing.com
*/
package parser

type tokener struct {
	stat []byte
	pos  int

	curToken   string
	flushToken bool

	err error
}

func newTokener(stat []byte) *tokener {
	return &tokener{
		stat, 0, "", true, nil,
	}
}

// Peek 查看下一个token, 不弹出
func (tk *tokener) Peek() (string, error) {
	if tk.err != nil {
		return "", tk.err
	}

	if tk.flushToken == true {
		token, err := tk.next()
		if err != nil {
			tk.err = err
			return "", err
		}
		tk.curToken = token
		tk.flushToken = false
	}

	return tk.curToken, nil
}

// Pop 弹出当前的token
func (tk *tokener) Pop() {
	tk.flushToken = true
}

func (tk *tokener) popByte() {
	tk.pos++
	if tk.pos > len(tk.stat) {
		tk.pos = len(tk.stat)
	}
}

func (tk *tokener) peekByte() (byte, bool) {
	if tk.pos == len(tk.stat) {
		return 0, true
	}
	return tk.stat[tk.pos], false
}

func (tk *tokener) next() (string, error) {
	if tk.err != nil {
		return "", tk.err
	}
	return tk.nextMetaState()
}

func (tk *tokener) nextMetaState() (string, error) {
	for {
		b, eof := tk.peekByte()
		if eof == true {
			return "", nil
		}
		if isBlank(b) == false {
			break
		}
		tk.popByte()
	}

	b, _ := tk.peekByte()
	if isSymbol(b) {
		tk.popByte()
		return string(b), nil
	} else if b == '"' || b == '\'' {
		return tk.nextQuoteState()
	} else if isAlphaBeta(b) || isDigital(b) {
		return tk.nextTokenState()
	} else {
		tk.err = ErrInvalidStat
		return "", tk.err
	}
}

func (tk *tokener) nextTokenState() (string, error) {
	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof == true || (isAlphaBeta(b) || isDigital(b)) == false {
			if isBlank(b) {
				tk.popByte()
			}
			return string(tmp), nil
		}
		tmp = append(tmp, b)
		tk.popByte()
	}
}

func (tk *tokener) nextQuoteState() (string, error) {
	quote, _ := tk.peekByte()
	tk.popByte()

	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof == true {
			tk.err = ErrInvalidStat
			return "", tk.err
		}
		if b == quote {
			tk.popByte()
			break
		}
		tmp = append(tmp, b)
		tk.popByte()
	}

	return string(tmp), nil
}

func (tk *tokener) errStat() []byte {
	tmp := make([]byte, len(tk.stat)+3)
	copy(tmp, tk.stat[:tk.pos])
	copy(tmp[tk.pos:], []byte("<< "))
	copy(tmp[tk.pos+3:], tk.stat[tk.pos:])
	return tmp
}

func isSymbol(b byte) bool {
	return b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')'
}

func isAlphaBeta(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isDigital(b byte) bool {
	return b >= '0' && b <= '9'
}

func isBlank(b byte) bool {
	return b == '\n' || b == ' ' || b == '\t'
}
