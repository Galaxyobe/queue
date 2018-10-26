package queue

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"strings"
	"sync"
	"fmt"
	"errors"
	"log"
)

// queue interface
type IQueue interface {
	Push(data interface{}) error
	Pop(timeout uint64) (interface{}, error)
	Remove(data interface{}) error
}

// mark is queue data
const signQueueData = "#|"

// queue data format
const signQueueDataFormat = "%s#|%v"

// queue data handle function
// return true will remove the data in the queue
type HandleFunc func(data string) bool

// queue inherit IQueue
// use a chan controller the max concurrency
type Queue struct {
	IQueue
	maxConcurrency chan struct{}
	typeHandleMap  sync.Map
}

// new queue with the IQueue and mac concurrency
func NewQueue(face IQueue, maxConcurrency int) *Queue {
	q := &Queue{
		IQueue:         face,
		maxConcurrency: make(chan struct{}, maxConcurrency),
	}

	return q
}

// get the type of the data return the type and raw data
func (q *Queue) GetDataType(data string) ([]string, error) {
	splits := strings.Split(data, signQueueData)
	if len(splits) < 2 {
		return nil, fmt.Errorf("%s have not match queue sign", data)
	}

	return splits, nil
}

// register data handle function with the name and then func
func (q *Queue) RegisterDataHandleFunc(name string, handleFunc HandleFunc) error {
	if name == "" {
		return errors.New("name can't be empty")
	}

	q.typeHandleMap.Store(name, handleFunc)

	return nil
}

// push a raw data and mark the name into the data
func (q *Queue) Push(name string, data interface{}) error {
	d := fmt.Sprintf(signQueueDataFormat, name, data)
	return q.IQueue.Push(d)
}

// pop the the data and parse the type, get the type function handle
// the raw data, when function return true will remove the record
func (q *Queue) Done(ctx context.Context) {
	id := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// pop data with timeout
		data, err := redis.String(q.Pop(1))
		if err != nil {
			continue
		}
		// is queue data ?
		if !strings.Contains(data, signQueueData) {
			continue
		}
		// put chan for one goroutine
		q.maxConcurrency <- struct{}{}
		id++
		log.Println(id, "<-", data)
		// run goroutine handle data
		go func(id int, data string) {

			defer func() {
				// get recover
				if e := recover(); e != nil {
				}
				log.Println(id, "->", data)
				// out chan
				<-q.maxConcurrency
			}()
			// get data type for convert the handle function
			dataType, err := q.GetDataType(data)
			if err != nil {
				return
			}
			if f, ok := q.typeHandleMap.Load(dataType[0]); ok {
				if handle, ok := f.(HandleFunc); ok {
					if handle(dataType[1]) {
						// handle succeed remove the data
						q.Remove(data)
						log.Println(id, "remove", data)
					}
				}
			}
		}(id, data)
	}
}
