package queue

import (
	"testing"
	"time"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/alicebob/miniredis"
	"context"
	"math/rand"
)

func TestQueue_Push(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", s.Addr())
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	queue := NewRedisQueue(pool, "TestQueue_Push")

	q := NewQueue(queue, 10)

	tpy := "time"
	now := time.Now()

	q.Push("time", now.Format(time.RFC3339Nano))

	data, err := s.Lpop("TestQueue_Push")
	if err != nil {
		t.Error(err)
	}

	typeData, err := q.GetDataType(data)
	if err != nil {
		t.Error(err)
	}
	if typeData[0] != tpy {
		t.Error("type should be", tpy, "not", typeData[0])
	}
	if n, err := time.Parse(time.RFC3339Nano, typeData[1]); err != nil {
		t.Error(err)
	} else if !n.Equal(now) {
		t.Error("value should be", now, "not", n)
	}
}

func TestQueue_GetDataType(t *testing.T) {
	tpy := "time"
	now := time.Now()
	data := fmt.Sprintf(signQueueDataFormat, tpy, now.Format(time.RFC3339Nano))
	q := NewQueue(nil, 0)

	typeData, err := q.GetDataType(data)
	if err != nil {
		t.Error(err)
	}
	if typeData[0] != tpy {
		t.Error("type should be", tpy, "not", typeData[0])
	}
	if n, err := time.Parse(time.RFC3339Nano, typeData[1]); err != nil {
		t.Error(err)
	} else if !n.Equal(now) {
		t.Error("value should be", now, "not", n)
	}
}

func TestQueue_RegisterDataHandleFunc(t *testing.T) {
	q := NewQueue(nil, 0)

	q.RegisterDataHandleFunc("test", func(s string) bool {
		if s == "true" {
			return true
		}
		return false
	})

	f, ok := q.typeHandleMap.Load("test")
	if !ok {
		t.Error("register data handle func failed")
	}

	fn, ok := f.(HandleFunc)
	if !ok {
		t.Error("register data handle func type is not HandleFunc")
	}

	if !fn("true") {
		t.Error("should be true")
	}

	if fn("false") {
		t.Error("should be false")
	}
}

func TestQueue_Done(t *testing.T) {

	s, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	defer s.Close()
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", s.Addr())
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	queue := NewRedisQueue(pool, "TestQueue_Done")

	q := NewQueue(queue, 10)

	tpy := "time"
	now := time.Now()

	q.Push("time", now.Format(time.RFC3339Nano))
	rand.Seed(time.Now().UnixNano())
	q.RegisterDataHandleFunc(tpy, func(s string) bool {
		if now, err := time.Parse(time.RFC3339Nano, s); err != nil {
			return false
		} else {
			n := rand.Int31n(5)
			time.Sleep(time.Second * time.Duration(n))
			if n, err := time.Parse(time.RFC3339Nano, s); err != nil {
				t.Error(err)
			} else if !n.Equal(now) {
				t.Error("value should be", now, "not", n)
			}
			return true
		}
		return false
	})

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

	ctx2, canceled2 := context.WithTimeout(context.Background(), time.Second*30)
	defer canceled2()

	if l, err := s.List(queue.key); err != nil {
		t.Error(err)
	} else if len(l) == 0 {
		t.Error("privateKey should have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}

	q.Done(ctx2)

	if l, err := s.List(queue.key); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}
}
