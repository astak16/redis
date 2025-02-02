package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"redis/resp/client"
)

type connectionFactory struct {
	Peer string
}

func (f connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	// 初始化 tcp 客户端
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	// 启动 tcp 客户端
	c.Start()
	// 返回连接池
	return pool.NewPooledObject(c), nil
}

func (f connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	// 关闭 tcp 客户端
	c.Close()
	return nil
}

func (f connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (f connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
