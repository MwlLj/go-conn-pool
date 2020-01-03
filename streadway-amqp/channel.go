package amqppool

import (
    "github.com/streadway/amqp"
    "time"
)

type CChannel struct {
    conn *CConnect
    channel *amqp.Channel
    closeNotify chan *amqp.Error
    timer *time.Timer
    channelTimeout time.Duration
}

func (self *CChannel) init() {
    go func(self *CChannel) {
        self.channel.NotifyClose(self.closeNotify)
        for {
            select {
            case <-self.closeNotify:
                // fmt.Println("channel close")
                self.conn.onChannelClose(self)
                break
            case <-self.timer.C:
                self.conn.onChannelClose(self)
                break
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
    self.conn.addChannelToFree(self)
}

func NewChannel(conn *CConnect, channel *amqp.Channel, timeout time.Duration) *CChannel {
    c := CChannel{
        conn: conn,
        channel: channel,
        timer: time.NewTimer(timeout),
        channelTimeout: timeout,
    }
    c.init()
    return &c
}
