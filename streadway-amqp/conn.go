package amqppool

import (
    "container/list"
    "errors"
    "github.com/streadway/amqp"
    "sync"
    "time"
)

type CConnect struct {
    pool *CAmqpConnPool
    max int
    total int
    totalMutex sync.Mutex
    freeChannels *list.List
    freeChanMutex sync.Mutex
    conn *amqp.Connection
    closeNotify chan *amqp.Error
    timer *time.Timer
    connTimeout time.Duration
    channelTimeout time.Duration
    createChannelCb CreateChannelCb
}

func (self *CConnect) init() {
    go func(self *CConnect) {
        /*
        ** 注册close通知
        */
        self.conn.NotifyClose(self.closeNotify)
        /*
        ** 启动监听
        */
        for {
            select {
            case <-self.closeNotify:
                /*
                ** 与rabbitmq断开连接
                */
                self.pool.connOnClose(self)
            case <-self.timer.C:
                /*
                ** 一定时间内没有被使用
                */
                self.pool.connOnClose(self)
            }
        }
    }(self)
}

func (self *CConnect) ResetTimer() {
    self.timer.Reset(self.connTimeout)
}

func (self *CConnect) TakeChannel() (*CChannel, error) {
    freeCount := self.freeChannels.Len()
    if freeCount == 0 {
        /*
        ** 空闲队列中不存在channel
        **  判断当前通道总数是否大于最大值
        **      大于等于最大值 => 返回错误
        **      小于最大值 => 创建通道
        */
        if self.total >= self.max {
            /*
            ** 返回错误
            */
            return nil, errors.New("max channel limit")
        } else {
            /*
            ** 1. 创建通道, 将total加1
            ** 2. 不存入空闲队列, 直接返回
            */
            channel, err := self.conn.Channel()
            if err != nil {
                return nil, err
            }
            if self.createChannelCb != nil {
                err = (self.createChannelCb)(channel)
                if err != nil {
                    channel.Close()
                    return nil, err
                }
            } else {
            }
            c := NewChannel(self, channel, self.channelTimeout)
            self.totalOpt(func(total *int) {
                *total += 1
            })
            /*
            ** 此时: 空闲队列中不存在元素, 所以 total 记录的个数是忙碌的个数
            ** 判断是否通知 pool
            */
            if self.total >= self.max {
                /*
                ** 忙碌的个数已经达到极限 => 通知pool
                */
                self.pool.removeConnFromFree(self)
            }
            return c, nil
        }
    } else {
        /*
        ** 空闲队列中存在channel
        **      1. 取出, 并返回第一个元素
        */
        self.freeChanMutex.Lock()
        channelElem := self.freeChannels.Front()
        channel := channelElem.Value.(*CChannel)
        channel.ResetTimer()
        self.freeChannels.Remove(channelElem)
        self.freeChanMutex.Unlock()
        return channel, nil
    }
}

/*
** CChannel调用
**  当调用方调用 CChannel 的 Close后, 调用该方法
*/
func (self *CConnect) addChannelToFree(c *CChannel) {
    /*
    ** 释放channel到队列中
    */
    self.freeChanMutex.Lock()
    self.freeChannels.PushBack(c)
    self.freeChanMutex.Unlock()
    /*
    ** 通知pool
    */
    self.pool.addConnToFree(self)
}

/*
** CChannel调用
**  CChannel接收到Close通知/长时间未使用 后, 调用该方法
*/
func (self *CConnect) onChannelClose(c *CChannel) {
    self.totalOpt(func(total *int) {
        *total -= 1
    })
    self.pool.addConnToFree(self)
}

func (self *CConnect) totalOpt(f func(total *int)) {
    self.totalMutex.Lock()
    defer self.totalMutex.Unlock()
    f(&self.total)
}

func NewConnect(pool *CAmqpConnPool, conn *amqp.Connection, max int, connTimeout time.Duration, channelTimeout time.Duration, createChannelCb CreateChannelCb) *CConnect {
    c := CConnect{
        pool: pool,
        max: max,
        freeChannels: list.New(),
        conn: conn,
        closeNotify: make(chan *amqp.Error),
        timer: time.NewTimer(connTimeout),
        connTimeout: connTimeout,
        channelTimeout: channelTimeout,
        createChannelCb: createChannelCb,
    }
    c.init()
    return &c
}
