package amqppool

import (
    "testing"
    "time"
    "fmt"
    "github.com/streadway/amqp"
)

func TestPublish(t *testing.T) {
    // t.SkipNow()
    url := "amqp://guest:guest@localhost:5672"
    pool := NewAmqpConnPool(10, 10, &url, time.Second * 60, time.Second * 10)
    pool.RegisterCreateConnectCb(func(conn *amqp.Connection) error {
        return nil
    })
    exchange := "test-exchange"
    exchangeType := "direct"
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
    for i := 0; i < 1000; i++ {
        go func() {
            for {
                conn, err := pool.TakeConn()
                if err != nil {
                    fmt.Println("TakeConn, ", err)
                    <-time.After(1 * time.Second)
                    continue
                }
                channel, err := conn.TakeChannel()
                if err != nil {
                    fmt.Println("TakeChannel, ", err)
                    <-time.After(1 * time.Second)
                    continue
                }
                routingKey := "test-key"
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
                channel.Close()
                fmt.Println("send success")
                <-time.After(1 * time.Second)
            }
        }()
    }
}

func TestConsumer(t *testing.T) {
    // t.SkipNow()
    url := "amqp://guest:guest@localhost:5672"
    pool := NewAmqpConnPool(10, 10, &url, time.Second * 60, time.Second * 10)
    pool.RegisterCreateConnectCb(func(conn *amqp.Connection) error {
        return nil
    })
    exchange := "test-exchange"
    exchangeType := "direct"
    queueName := "test-queue"
    routingKey := "test-key"
    tag := "simple_consumer"
    pool.RegisterCreateChannelCb(func(channel *amqp.Channel) error {
        fmt.Println("on create channel")
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
        queue, err := channel.QueueDeclare(
            queueName,
            true,      // durable
            false,     // delete when unused
            false,     // exclusive
            false,     // noWait
            nil,       // arguments
        )
        if err != nil {
            return err
        }
        if err = channel.QueueBind(
            queue.Name, // name of the queue
            routingKey,        // bindingKey
            exchange,   // sourceExchange
            false,      // noWait
            nil,        // arguments
        ); err != nil {
            return err
        }
        return nil
    })

    for {
        fmt.Println("take")
        conn, err := pool.TakeConn()
        if err != nil {
            fmt.Println(err)
            <-time.After(time.Second * 1)
            continue
        }
        c, err := conn.TakeChannel()
        if err != nil {
            fmt.Println(err)
            <-time.After(time.Second * 1)
            continue
        }
        channel := c.Channel()
        deliveries, err := channel.Consume(
            queueName, // name
            tag,      // consumerTag,
            false,      // noAck
            false,      // exclusive
            false,      // noLocal
            false,      // noWait
            nil,        // arguments
        )
        if err != nil {
            fmt.Println(err)
            <-time.After(time.Second * 1)
            continue
        }
        for d := range deliveries {
            fmt.Println("recv...")
            fmt.Printf(
                "got %dB delivery: [%v] %q\n",
                len(d.Body),
                d.DeliveryTag,
                d.Body,
            )
            d.Ack(false)
        }
        fmt.Println("exit recv loop")
        c.Close()
        <-time.After(time.Second * 1)
    }
}
