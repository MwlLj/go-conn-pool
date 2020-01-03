package amqppool

/*
** https://github.com/streadway/amqp 创建连接池
*/
import (
    "github.com/streadway/amqp"
    "container/list"
    "errors"
    "sync"
    "time"
    "math/rand"
    "fmt"
)

var _ = fmt.Println

type CreateConnectCb func(conn *amqp.Connection) error
type CreateChannelCb func(channel *amqp.Channel) error

type CAmqpConnPool struct {
    /*
    ** 空闲 + 工作
    */
    max int
    channelMax int
    total int
    totalMutex sync.Mutex
    freeConns *list.List
    freeConnsMutex sync.Mutex
    url *string
    connectTimeout time.Duration
    channelTimeout time.Duration
    createConnectCb CreateConnectCb
    createChannelCb CreateChannelCb
    connMutex sync.Mutex
}

func (self *CAmqpConnPool) TakeConn() (*CConnect, error) {
    self.connMutex.Lock()
    defer self.connMutex.Unlock()
    freeCount := self.freeConns.Len()
    // fmt.Println(self.total, freeCount)
    if freeCount == 0 {
        /*
        ** 空闲队列中不存在连接
        **  判断当前连接总数是否大于最大值
        **      大于等于最大值 => 返回错误
        **      小于最大值 => 创建连接
        */
        if self.total > self.max {
            /*
            ** 返回错误
            */
            return nil, errors.New("max conn limit")
        } else {
            /*
            ** 1. 创建连接, 将total加1
            ** 2. 放入到空闲队列中
            */
            // fmt.Println(self.total, freeCount, "############")
            c, err := amqp.Dial(*self.url)
            if err != nil {
                return nil, err
            }
            if self.createConnectCb != nil {
                err = (self.createConnectCb)(c)
                if err != nil {
                    c.Close()
                    return nil, err
                }
            } else {
            }
            conn := NewConnect(self, c, self.channelMax, self.connectTimeout, self.channelTimeout, self.createChannelCb)
            self.freeConnsMutex.Lock()
            self.freeConns.PushBack(conn)
            self.freeConnsMutex.Unlock()
            self.totalOpt(func(total *int) {
                *total += 1
            })
            return conn, nil
        }
    } else {
        /*
        ** 空闲队列中存在连接
        **  随机选择一个连接 (不从队列中移除) (由 CConnect 调用接口移除)
        */
        randIndex := rand.Intn(freeCount)
        i := 0
        for e := self.freeConns.Front(); e != nil; e = e.Next() {
            if i == randIndex {
                conn := e.Value.(*CConnect)
                conn.ResetTimer()
                return conn, nil
            }
            i += 1
        }
        return nil, errors.New("rand get conn error")
    }
}

func (self *CAmqpConnPool) Close() {
    for e := self.freeConns.Front(); e != nil; e = e.Next() {
        e.Value.(*CConnect).close()
    }
    for {
        e := self.freeConns.Front()
        if e == nil {
            break
        }
        self.freeConns.Remove(e)
    }
}

/*
** CConnect对象调用
**  当conn管理的所有的channel都忙碌的时候, 调用该方法
*/
func (self *CAmqpConnPool) removeConnFromFree(conn *CConnect) {
    for e := self.freeConns.Front(); e != nil; e = e.Next() {
        if e.Value.(*CConnect) == conn {
            self.freeConnsMutex.Lock()
            self.freeConns.Remove(e)
            self.freeConnsMutex.Unlock()
            break
        }
    }
}

/*
** CConnect调用
**  CConnect接收到Close通知/长时间未使用 后, 调用该方法
*/
func (self *CAmqpConnPool) connOnClose(conn *CConnect) {
    /*
    ** 1. 总数减1
    ** 2. 如果存在空闲队列中, 需要移除
    */
    self.totalOpt(func(total *int) {
        *total -= 1
    })
    for e := self.freeConns.Front(); e != nil; e = e.Next() {
        if e.Value.(*CConnect) == conn {
            self.freeConnsMutex.Lock()
            self.freeConns.Remove(e)
            self.freeConnsMutex.Unlock()
            break
        }
    }
}

/*
** CConnnect对象调用
**  当conn管理的所有的channel存在空闲时, 调用该方法
*/
func (self *CAmqpConnPool) addConnToFree(conn *CConnect) {
    /*
    ** 判断 conn 是否在空闲队列中, 如果不存在, 才可以添加到最后
    */
    for e := self.freeConns.Front(); e != nil; e = e.Next() {
        if e.Value.(*CConnect) == conn {
            return
        }
    }
    self.freeConns.PushBack(conn)
}

func (self *CAmqpConnPool) totalOpt(f func(total *int)) {
    self.totalMutex.Lock()
    defer self.totalMutex.Unlock()
    f(&self.total)
}

func (self *CAmqpConnPool) RegisterCreateConnectCb(cb CreateConnectCb) {
    self.createConnectCb = cb
}

func (self *CAmqpConnPool) RegisterCreateChannelCb(cb CreateChannelCb) {
    self.createChannelCb = cb
}

func NewAmqpConnPool(max int, channelMax int, url *string, connectTimeout time.Duration, channelTimeout time.Duration) *CAmqpConnPool {
    pool := CAmqpConnPool {
        max: max,
        channelMax: channelMax,
        freeConns: list.New(),
        url: url,
        connectTimeout: connectTimeout,
        channelTimeout: channelTimeout,
    }
    return &pool
}
