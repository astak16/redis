package parser

import (
	"bufio"
	"errors"
	"io"
	"redis/interface/resp"
	"redis/lib/logger"
	"redis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	// 用户发送的指令和回复给用户的指令格式一样
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool     // 正在解析的数据是单行还是多行
	expectedArgsCount int      // 正在读取的指令有几个参数
	msgType           byte     // 传过来的消息类型
	args              [][]byte // 传过来的数据本身
	bulkLen           int64    // 数据块的长度
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parser0(reader, ch)
	return ch
}

func parser0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		// 读到 io.EOF，表示数据读取完毨，跳出 for 循环
		// 如果是 io.EOF，ioErr 变量值为 true
		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			// 如果 redis 协议解析出错，返回一个错误回复即可，无需结束程序
			ch <- &Payload{Err: err}
			state = readState{}
			continue
		}
		if !state.readingMultiLine {
			// 解析 * 开头的指令
			if msg[0] == '*' {
				err := parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: errors.New("protocol error:" + string(msg))}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{Data: reply.MakeEmptyMultiBulkReply()}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				// 解析 $ 开头的指令
				err := parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: errors.New("protocol error:" + string(msg))}
					state = readState{}
					continue
				}
				if state.bulkLen == -1 {
					ch <- &Payload{Data: reply.MakeNullBulkReply()}
					state = readState{}
					continue
				}
			} else {
				// 其他指令，比如 +OK\r\n -ERR\r\n 这种
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			// 每一个循环都会进来，将 msg 交给 readBody 函数处理
			// readBody 函数会将 msg 解析成一个个的数据，然后添加到 state.args 中
			err := readBody(msg, &state)
			if err != nil {
				ch <- &Payload{Err: errors.New("protocol error:" + string(msg))}
				state = readState{}
				continue
			}
			// 当解析完成之后，将解析的数据返回给用户
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					// * 表示要返回一个数组
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					// $ 表示要返回一个字符串
					result = reply.MakeBulkReply(state.args[0])
				}
				// 将数据通过 chan 返回给上层
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	// 1. \r\n 切分
	// state.bulkLen == 0，表示没有预设的个数，直接读取数据
	if state.bulkLen == 0 {
		// 读到 \n 结束
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		// \n 前面是不是 \r，如果不是，说明数据有问题
		// 数据没有长度，说明数据也有问题
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error:" + string(msg))
		}
	} else {
		// 2. 如果有 $数字，按照数字读取字符
		// 读取 SET\r\n，ke\r\ny\r\n 这样的内容
		// 所以这个长度是 set 长度+ \r\n 长度
		msg = make([]byte, state.bulkLen+2)
		// io.ReadFull 读取指定长度的数据，塞满 msg
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, nil
		}
		// 判断数据是否有问题
		// 数据长度不对
		// 倒数第一个字符是不是 \n，倒数第二个字符是不是 \r
		if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error:" + string(msg))
		}
		// 读完数据之后，把 bulkLen 设置为 0，下次读取数据的时候，就会直接读取数据
		state.bulkLen = 0
	}
	return msg, false, nil
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	// 将 *3\r\n 的 3 解析出来
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error:" + string(msg))
	}
	// 如果 expectedLine == 0，表示没有数据，直接返回
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// 如果 expectedLine > 0，表示有数据，设置状态
		// 设置数据的长度
		state.msgType = msg[0]
		// 设置读取的数据是多行
		state.readingMultiLine = true
		// 设置数据的个数
		state.expectedArgsCount = int(expectedLine)
		// 设置数据的长度
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error:" + string(msg))
	}
}

// $4\r\nPING\r\n
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error:" + string(msg))
	}
	// 处理 $-1\r\n
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		// 这种情况也是读取多行数据
		// $4\r\nPING\r\n，有两组 \r\n
		state.readingMultiLine = true
		// 只需要接收 ping 一个参数
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error:" + string(msg))
	}
}

// +OK\r\n -ERR\r\n :5\r\n
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	// 把 \r\n 去掉
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	// 拿到第一个字符，判断是什么类型的数据
	switch msg[0] {
	case '+':
		// 拿到 + 后面的数据，返回一个状态回复
		result = reply.MakeStatusReply(str[1:])
	case '-':
		// 拿到 - 后面的数据，返回一个标准错误回复
		result = reply.MakeErrReply(str[1:])
	case ':':
		// 拿到 : 后面的数据，返回一个数字回复
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error:" + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n，*3\r\n 已经被解析了，剩余未解析的 $3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// $4\r\nPING\r\n PING\r\n，$4\r\n 已经被解析了，剩余未解析的 PING\r\n
func readBody(msg []byte, state *readState) error {
	// 切掉 \r\n
	line := msg[0 : len(msg)-2]
	var err error
	// $3 这样的指令
	// 如果第一个字符是 $，说明后面是描述数据的长度
	if line[0] == '$' {
		// 拿到 $ 后面的数据，解析数据的长度
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error:" + string(msg))
		}
		// 如果数据长度 <= 0，表示数据为空
		if state.bulkLen <= 0 {
			// 往 state.args 中添加一个空数据
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		// SET 这样的指令
		state.args = append(state.args, line)
		// 这里不需要设置 bulkLen，因为在上面那个分支里已经设置了
	}
	return nil
}
