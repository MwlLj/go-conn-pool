package amqppool

import (
    "github.com/streadway/amqp"
    "time"
    "sync"
)

type CChannel struct {
    conn *CConnect
    channel *amqp.Channel
    closeNotify chan *amqp.Error
    timer *time.Timer
    channelTimeout time.Duration
    exit chan bool
    isClose bool
    isCloseMutex sync.Mutex
}

func (self *CChannel) init() {
    go func(self *CChannel) {
        self.channel.NotifyClose(self.closeNotify)
        for {
            select {
            case <-self.closeNotify:
                // fmt.Println("channel close")
                self.conn.onChannelClose(self)
                self.setIsClose(true)
                return
            case <-self.timer.C:
                self.conn.onChannelClose(self)
                self.setIsClose(true)
                return
            case <-self.exit:
                self.setIsClose(true)
                return
            }
        }
    }(self)
}

func (self *CChannel) ResetTimer() {
    // fmt.Println("reset timer ...................................", self.channelTimeout)
    self.timer.Reset(self.channelTimeout)
}

func (self *CChannel) Channel() *amqp.Channel {
    return self.channel
}

func (self *CChannel) Close() {
    if self.getIsClose() {
        /*
        ** 此时: 当前channel 刚刚忙碌完毕
        ** 希望放回到空闲队列中, 但是如果自身已经关闭, 就没有放回空闲队列的价值
        */
        return
    }
    self.conn.addChannelToFree(self)
}

func (self *CChannel) close() {
    self.exit <- true
}

func (self *CChannel) getIsClose() bool {
    self.isCloseMutex.Lock()
    defer self.isCloseMutex.Unlock()
    return self.isClose
}

func (self *CChannel) setIsClose(isClose bool) {
    self.isCloseMutex.Lock()
    defer self.isCloseMutex.Unlock()
    self.isClose = isClose
}

func NewChannel(conn *CConnect, channel *amqp.Channel, timeout time.Duration) *CChannel {
    c := CChannel{
        conn: conn,
        channel: channel,
        timer: time.NewTimer(timeout),
        channelTimeout: timeout,
        exit: make(chan bool),
    }
    c.init()
    return &c
}
