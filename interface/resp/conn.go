package resp

type Connection interface {
	Write([]byte) error // 写入数据
	GetDBIndex() int    // 获取 db 的
	SelectDB(int)       // 切换 redis 数据库
}
