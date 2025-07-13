package main

import (
	"fmt"
	"time"
)

type BufferedWorker[T any] struct {
	maxItems      int
	flushInterval time.Duration
	callback      func([]T)
	items         []T
}

func NewBufferedWorker[T any](maxItems int, flushInterval time.Duration, callback func([]T)) *BufferedWorker[T] {
	return &BufferedWorker[T]{
		maxItems:      maxItems,
		flushInterval: flushInterval,
		callback:      callback,
		items:         make([]T, 0, maxItems),
	}
}

func (b *BufferedWorker[T]) Start() chan<- T {
	itemCh := make(chan T)
	ticker := time.NewTicker(b.flushInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				b.flush()
			case item := <-itemCh:
				b.items = append(b.items, item)
				if len(b.items) >= b.maxItems {
					b.flush()
				}
			}
		}
	}()

	return itemCh
}

func (b *BufferedWorker[T]) flush() {
	b.callback(b.items)
	b.items = b.items[:0]
}
func main() {
	worker := NewBufferedWorker(4, time.Second*5, func(t []int) {
		fmt.Printf("Flushed %v\n", t)
	})

	itemCh := worker.Start()

	itemCh <- 5
	itemCh <- 5
	itemCh <- 5
	itemCh <- 5
	itemCh <- 5

	time.Sleep(7 * time.Second)
}
