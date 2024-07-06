package connection

import (
	"net"
	"redis/lib/sync/wait"
	"sync"
	"time"
)

type Connection struct {
	conn         net.Conn   // 连接信息
	waitingReply wait.Wait  // 在关掉 server 之前，需要等待回复给用户的指令完成
	mu           sync.Mutex // 在操作 conn 的时候，需要加锁
	selectedDB   int        // 现在用户在操作哪个 db
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{conn: conn}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// 关闭连接直接调用 conn.Close() 即可
// 但是在关闭连接之前，需要等待回复给用户的指令完成
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// 给用户回复消息
func (c *Connection) Write(bytes []byte) error {
	// 如果没有数据，直接返回
	if len(bytes) == 0 {
		return nil
	}
	// 写入数据的时候，需要加锁
	c.mu.Lock()
	// 每次写入数据，等待回复的指令 +1
	c.waitingReply.Add(1)
	defer func() {
		// 写入数据完成之后，等待回复的指令 -1
		c.waitingReply.Done()
		// 解锁
		c.mu.Unlock()
	}()
	// 写入数据
	_, err := c.conn.Write(bytes)
	return err
}

// 用户正在使用哪个 db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// 用户选择自己想用的 db
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
