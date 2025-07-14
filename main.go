package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type BufferedWorker[T any] struct {
	maxItems      int
	flushInterval time.Duration
	callback      func([]T)
	items         []T
	itemCh        chan T
}

func NewBufferedWorker[T any](maxItems int, flushInterval time.Duration, callback func([]T)) *BufferedWorker[T] {
	return &BufferedWorker[T]{
		maxItems:      maxItems,
		flushInterval: flushInterval,
		callback:      callback,
		items:         make([]T, 0, maxItems),
		itemCh:        make(chan T),
	}
}

func (b *BufferedWorker[T]) Run(ctx context.Context) error {
	tickerCh := time.Tick(b.flushInterval)

	var wg sync.WaitGroup

	defer close(b.itemCh)
	for {
		select {
		case <-tickerCh:
			if len(b.items) > 0 {
				b.flushAsync(&wg)
			}
		case item := <-b.itemCh:
			b.items = append(b.items, item)
			if len(b.items) >= b.maxItems {
				b.flushAsync(&wg)
				tickerCh = time.Tick(b.flushInterval)
			}
		case <-ctx.Done():
			if len(b.items) > 0 {
				b.finalFlush()
			}

			wg.Wait()
			return ctx.Err()
		}
	}
}

func (b *BufferedWorker[T]) Add(item T) {
	fmt.Printf("adding %v\n", item)
	b.itemCh <- item	
}

func (b *BufferedWorker[T]) flushAsync(wg *sync.WaitGroup) {
	flushedItems := make([]T, len(b.items))
	copy(flushedItems, b.items)

	wg.Add(1)
	go func() {
		defer wg.Done()
		b.callback(flushedItems)
	}()
	
	b.items = b.items[:0]
}

func (b *BufferedWorker[T]) finalFlush() {
	fmt.Println("final flushing", b.items)
	b.callback(b.items)
}

func main() {
	fmt.Println("Number of Goroutines:", runtime.NumGoroutine())

	ctx, cancelCtx := context.WithCancel(context.Background())
	worker := NewBufferedWorker(10, time.Second*5, func(t []int) {
		time.Sleep(5 * time.Second)
		fmt.Printf("Flushed %v\n", t)
	})

	done := make(chan struct{})
	errc := make(chan error, 1)
	go func(){
		defer close(done)
		errc <- worker.Run(ctx)
	}()

	for n := range 21 {
		worker.Add(n)	
	}

	fmt.Println("shutting down")
	cancelCtx()

	fmt.Println("waiting for worker to flush everything")
	<-done

	fmt.Println("Number of Goroutines:", runtime.NumGoroutine())
}
