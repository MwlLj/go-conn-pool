package amqppool

import (
    "testing"
    "time"
    "fmt"
    "github.com/streadway/amqp"
)

func createPool() (exchange string, exchangeType string, pool *CAmqpConnPool) {
    url := "amqp://guest:guest@localhost:5672"
    pool = NewAmqpConnPool(10, 10, &url, time.Second * 60, time.Second * 10)
    pool.RegisterCreateConnectCb(func(conn *amqp.Connection) error {
        return nil
    })
    exchange = "test-exchange"
    exchangeType = "direct"
    pool.RegisterCreateChannelCb(func(channel *amqp.Channel) error {
        if err := channel.ExchangeDeclare(
            exchange,
            exchangeType,
            true,
            false,
            false,
            false,
            nil,
        ); err != nil {
            return err
        }
        return nil
    })
    return
}

func TestPublish(t *testing.T) {
    // t.SkipNow()
    exchange, _, pool := createPool()
    conn, err := pool.TakeConn()
    if err != nil {
        fmt.Println(err)
        return
    }
    channel, err := conn.TakeChannel()
    if err != nil {
        fmt.Println(err)
        return
    }
    routingKey := "test-key-0"
    body := "hello"
    if err = channel.Channel().Publish(
        exchange,   // publish to an exchange
        routingKey, // routing to 0 or more queues
        false,      // mandatory
        false,      // immediate
        amqp.Publishing{
            Headers:         amqp.Table{},
            ContentType:     "text/plain",
            ContentEncoding: "",
            Body:            []byte(body),
            DeliveryMode:    amqp.Transient,
            Priority:        0,
        },
    ); err != nil {
        fmt.Printf("Exchange Publish: %v\n", err)
    }
    var _ = channel
    var _ = conn
}
