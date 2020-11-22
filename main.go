package main


import (
"context"
"fmt"
"github.com/makasim/amqpextra"
"github.com/makasim/amqpextra/consumer"
"github.com/makasim/amqpextra/publisher"
"github.com/streadway/amqp"
"log"
"time"
)


const dsn = "amqp://guest:guest@localhost:5672"

func main()  {
	RunEchoServer(dsn, "a_reply_queue", false)
}

type logger struct{}

func(logger) Printf(format string, v... interface{}) {
	fmt.Printf(format + "\n", v...)
}

func RunEchoServer(dsn, queue string, declare bool)  {
	fmt.Println("RunEchoServer started")
	conn, err := amqpextra.NewDialer(amqpextra.WithURL(dsn))
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	pub, err := amqpextra.NewPublisher(conn.ConnectionCh())
	if err != nil {
		log.Fatal(err)
	}

	readyCh := make(chan consumer.Ready, 1)
	unreadyCh := make(chan error, 1)

	wait := make(chan struct{}, 1)
	i := 0
	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		if i == 1000 {
			wait <- struct{}{}
		}
		fmt.Printf("RunEchoServer handler body %s; reply to: %s\n", string(msg.Body), msg.ReplyTo)
		pub.Publish(publisher.Message{
			Key: msg.ReplyTo,
			Publishing: amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body:          []byte(string(msg.Body) + " modified by rpc server"),
			},
		})

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}

		return nil


	})

	c, err := amqpextra.NewConsumer(
		conn.ConnectionCh(),
		consumer.WithContext(ctx),
		consumer.WithHandler(h),
		consumer.WithLogger(logger{}),
		consumer.WithNotify(readyCh, unreadyCh),
		consumer.WithQueue(queue),
		consumer.WithWorker(consumer.NewParallelWorker(10)),
	)
	if err != nil {
		log.Fatal(err)
	}

	close(wait)

loop: for {
	select {
	case ready := <-readyCh:
		fmt.Println(ready.Queue)
	case <-wait:
		break loop
	}
}
	<- c.NotifyClosed()
	fmt.Println("RunEchoServer stopped")
}





