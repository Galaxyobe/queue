package queue

import (
	"github.com/gomodule/redigo/redis"
	"fmt"
	"time"
	"strings"
	"strconv"
)

// use redis list as queue
// use redis list BRPOPLPUSH to keep the data safe
type RedisQueue struct {
	// queue private key
	privateKey string
	// redis key
	key string
	// redis poll
	pool *redis.Pool
}

// new redis queue with redis pool and key name
// use linux timestamp for distinguish distributed read and write by privateKey
func NewRedisQueue(pool *redis.Pool, key string) *RedisQueue {
	q := &RedisQueue{
		// format is [KEY*]:@[timestamp]
		privateKey: fmt.Sprintf("%s:@%d", key, time.Now().Unix()),
		key:        key,
		pool:       pool,
	}

	return q
}

// return the private key to the queue key greater than interval second
func (q *RedisQueue) ReturnElements(interval int64) error {
	// redis conn
	conn := q.pool.Get()
	defer conn.Close()

	for {
		// scan the keys match queue key
		data, err := redis.Values(conn.Do("SCAN", 0, "MATCH", q.key+"*:@*", "COUNT", 100))

		if err != nil {
			return err
		}
		// parse the reply data
		cursor, _ := redis.Uint64(data[0], nil)
		keys, _ := redis.Strings(data[1], nil)
		// no key break
		if len(keys) == 0 {
			break
		}
		// range keys to return the elements to queue key
		for _, key := range keys {
			// split key and get the timestamp format is [KEY*]:@[timestamp]
			splits := strings.Split(key, ":@")
			timestamp, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				continue
			}
			// just return the key greater than interval
			if time.Now().Unix()-timestamp < interval && interval > 0 {
				continue
			}

			for {
				// pop and push key into queue key with 1s timeout
				reply, err := conn.Do("RPOPLPUSH", key, q.key)
				if reply == nil || err != nil {
					break
				}
			}
		}

		// get the cursor is 0 break loop
		if cursor == 0 {
			break
		}
	}
	return nil
}

// push any data into the queue
func (q *RedisQueue) Push(data interface{}) error {
	// redis conn
	conn := q.pool.Get()
	defer conn.Close()

	// inserting data at the end of the redis list
	_, err := conn.Do("LPUSH", q.key, data)

	return err
}

// pop first element with timeout
// pop from queue key into the queue private key
// finish handle the Pop element call the Remove function to remove
func (q *RedisQueue) Pop(timeout uint64) (interface{}, error) {
	// redis conn
	conn := q.pool.Get()
	defer conn.Close()

	// pop up the first element of the list.
	return conn.Do("BRPOPLPUSH", q.key, q.privateKey, timeout)
}

// remove the last element of private key
// if private key list is empty will get the redis ErrNil error
func (q *RedisQueue) Remove() error {
	// redis conn
	conn := q.pool.Get()
	defer conn.Close()

	// pop up the first element of the list.
	reply, err := conn.Do("RPOP", q.privateKey)

	if reply == nil {
		return redis.ErrNil
	}

	return err
}
