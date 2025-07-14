package main

import (
	"context"
	"fmt"
	"runtime"
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

func (b *BufferedWorker[T]) Start(ctx context.Context) (chan<- T, <-chan struct{}){
	itemCh := make(chan T)
	done := make(chan struct{})
	tickerCh := time.Tick(b.flushInterval)

	go func() {
		defer close(itemCh)
		defer close(done)
		defer fmt.Println("worker stopped")
		for {
			select {
			case <-tickerCh:
				b.flush()
			case item := <-itemCh:
				b.items = append(b.items, item)
				if len(b.items) >= b.maxItems {
					b.flush()
				}
			case <-ctx.Done():
				// TODO: flush the rest
				return
			}
		}
	}()

	return itemCh, done
}

func (b *BufferedWorker[T]) flush() {
	b.callback(b.items)
	b.items = b.items[:0]
}

func main() {
	fmt.Println("Number of Goroutines:", runtime.NumGoroutine())
	worker := NewBufferedWorker(4, time.Second*5, func(t []int) {
		fmt.Printf("Flushed %v\n", t)
	})

	ctx, cancelCtx := context.WithCancel(context.Background())
	itemCh, done := worker.Start(ctx)

	itemCh <- 5
	itemCh <- 5
	itemCh <- 5
	itemCh <- 5
	itemCh <- 5

	time.Sleep(4 * time.Second)
	cancelCtx()
	<-done
}
