package main

import (
	"github.com/galaxyobe/queue"
	"github.com/gomodule/redigo/redis"
	"time"
	"log"
	"context"
)

func main() {
	// new redis pool
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	// new redis queue
	redisQueue := queue.NewRedisQueue(pool, "queue_example")

	// return back all private to queue key
	redisQueue.ReturnElements(0)

	// new queue with redis queue and 10 handle concurrency
	q := queue.NewQueue(redisQueue, 10)

	// register data handle function
	q.RegisterDataHandleFunc("time", func(s string) bool {
		if now, err := time.Parse(time.RFC3339Nano, s); err != nil {
			return false
		} else {
			log.Println(now)
		}
		return true
	})

	// push data
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				q.Push("time", time.Now().Format(time.RFC3339Nano))
				time.Sleep(time.Second)
			}
		}
	}()

	// queue done
	q.Done(context.Background())
}
