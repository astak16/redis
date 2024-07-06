package reply

// 未知错误
type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (u UnknownErrReply) Error() string {
	return "Err unknown"
}

func (u UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func MakeUnknownErrReply() *UnknownErrReply {
	return &UnknownErrReply{}
}

// 参数错误
type ArgNumErrReply struct {
	Cmd string
}

func (r *ArgNumErrReply) Error() string {
	return "-ERR wrong number of arguments for '" + r.Cmd + "' command"
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{Cmd: cmd}
}

// 语法错误
//type SyntaxErrReply struct{}
//
//var syntaxErrBytes = []byte("-Err syntax error\r\n")
//var theSyntaxReply = &SyntaxErrReply{}
//
//func MakeSyntaxErrReply() *SyntaxErrReply {
//	return theSyntaxReply
//}
//
//func (r *SyntaxErrReply) Error() string {
//	return "Err syntax error"
//}
//
//func (r *SyntaxErrReply) ToBytes() []byte {
//	return syntaxErrBytes
//}

// 数据类型错误
//type WrongTypeErrReply struct{}
//
//var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
//
//func (r *WrongTypeErrReply) ToBytes() []byte {
//	return wrongTypeErrBytes
//}
//
//func (r *WrongTypeErrReply) Error() string {
//	return "WRONGTYPE Operation against a key holding the wrong kind of value"
//}

// 协议错误
//type ProtocolErrReply struct{ Msg string }
//
//func (r *ProtocolErrReply) ToBytes() []byte {
//	return []byte("-ERR Protocol error:'" + r.Msg + "'\r\n")
//}
//
//func (r *ProtocolErrReply) Error() string {
//	return "ERR Protocol error:'" + r.Msg + "'"
//}
