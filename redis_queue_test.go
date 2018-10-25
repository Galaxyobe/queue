package queue

import (
	"testing"
	"github.com/gomodule/redigo/redis"
	"time"
	"github.com/alicebob/miniredis"
)

func TestRedisQueue_Push(t *testing.T) {

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

	now := time.Now()

	queue := NewRedisQueue(pool, "TestRedisQueue_Push")
	if err := queue.Push(now.Format(time.RFC3339Nano)); err != nil {
		t.Error(err)
	}

	data, err := s.Lpop("TestRedisQueue_Push")
	if err != nil {
		t.Error(err)
	}

	if n, err := time.Parse(time.RFC3339Nano, data); err != nil {
		t.Error(err)
	} else if !n.Equal(now) {
		t.Error("value should be", now, "not", n)
	}
}

func TestRedisQueue_Pop(t *testing.T) {
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

	now := time.Now()

	queue := NewRedisQueue(pool, "TestRedisQueue_Pop")
	if err := queue.Push(now.Format(time.RFC3339Nano)); err != nil {
		t.Error(err)
	}

	str, err := redis.String(queue.Pop(1))
	if err != nil {
		t.Error(err)
	}

	if n, err := time.Parse(time.RFC3339Nano, str); err != nil {
		t.Error(err)
	} else if !n.Equal(now) {
		t.Error("value should be", now, "not", n)
	}

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) == 0 {
		t.Error("privateKey should have some data", l)
	}

	queue.Remove()

	if l, err := s.List(queue.privateKey); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}
}

func TestRedisQueue_ReturnElements(t *testing.T) {
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

	now := time.Now()

	queue := NewRedisQueue(pool, "TestRedisQueue_ReturnElements")
	if err := queue.Push(now.Format(time.RFC3339Nano)); err != nil {
		t.Error(err)
	}

	str, err := redis.String(queue.Pop(1))
	if err != nil {
		t.Error(err)
	}

	if n, err := time.Parse(time.RFC3339Nano, str); err != nil {
		t.Error(err)
	} else if !n.Equal(now) {
		t.Error("value should be", now, "not", n)
	}

	if l, err := s.List(queue.key); err != nil && err != miniredis.ErrKeyNotFound {
		t.Error(err)
	} else if len(l) != 0 {
		t.Error("privateKey should not have some data", l)
	}

	if l, err := s.List(queue.privateKey); err != nil {
		t.Error(err)
	} else if len(l) == 0 {
		t.Error("privateKey should have some data", l)
	}

	if err := queue.ReturnElements(0); err != nil {
		t.Error(err)
	}

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
}
