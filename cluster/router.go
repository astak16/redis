package cluster

import "redis/interface/resp"

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["ping"] = ping
	routerMap["renamenx"] = rename
	routerMap["rename"] = rename
	routerMap["flushdb"] = flushdb
	routerMap["del"] = del
	routerMap["select"] = execSelect
	return routerMap
}

// GET K1
// SET K1 V1
func defaultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 获取用户输入的 key
	key := string(cmdArgs[1])
	// 选择节点
	peer := cluster.peerPicker.PickNode(key)
	// 转发指令
	return cluster.relay(peer, c, cmdArgs)
}
