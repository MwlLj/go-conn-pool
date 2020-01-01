package amqppool

/*
** https://github.com/streadway/amqp 创建连接池
*/
import (
    "github.com/streadway/amqp"
)

type CAmqpConnPool struct {
    /*
    ** 空闲 + 工作
    */
    max int
    total int
    freeConns []*CConnect
}

func (self *CAmqpConnPool) TakeConn() (*CConnect, error) {
    freeCount := len(self.freeConns)
    if freeCount == 0 {
        /*
        ** 空闲队列中不存在连接
        **  判断当前连接总数是否大于最大值
        **      大于等于最大值 => 返回错误
        **      小于最大值 => 创建连接
        ** 空闲队列中存在连接
        **  返回数据的第一个元素
        */
    }
}

func NewAmqpConnPool(max int) *CAmqpConnPool {
    pool := CAmqpConnPool {
        max: max,
    }
    return &pool
}
