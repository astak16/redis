package resp

type Reply interface {
	ToBytes() []byte // 把回复的内容转成字节
}
