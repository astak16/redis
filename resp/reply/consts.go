package reply

// pong
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

func (p *PongReply) ToBytes() []byte {
	return pongBytes
}
func MakePongReply() *PongReply {
	return &PongReply{}
}

// ok
type OkReply struct{}

var theOkReply = new(OkReply)
var okBytes = []byte("+OK\r\n")

func (r *OkReply) ToBytes() []byte {
	return okBytes
}
func MakeOkReply() *OkReply {
	return &OkReply{}
}

// null
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

func (n NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// 空数组
type EmptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (e EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

// 空
//type NoReply struct{}
//
//var noReplyBytes = []byte("")
//
//func (n NoReply) ToBytes() []byte {
//	return noReplyBytes
//}
//
//func MakeNoReply() *NoReply {
//	return &NoReply{}
//}
