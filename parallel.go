package pump

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Parallel constructs a stage function that invokes the given stage from n
// goroutines in parallel. The value of n has the upper bound of 100 * runtime.NumCPU().
// Zero or negative value of n sets the number of goroutines to the result of
// calling [runtime.NumCPU]. This stage does not preserve the order of data items.
func Parallel[T, U any](n int, stage Stage[T, U]) Stage[T, U] {
	return ParallelCtx(context.Background(), n, stage)
}

// ParallelCtx constructs a stage function that invokes the given stage from n
// goroutines in parallel, under control of the given context. The value of n has
// the upper bound of 100 * runtime.NumCPU(). Zero or negative value of n sets the number
// of goroutines to the result of calling [runtime.NumCPU]. This stage does not preserve
// the order of data items.
func ParallelCtx[T, U any](ctx context.Context, n int, stage Stage[T, U]) Stage[T, U] {
	// ensure a realistic value for n
	np := runtime.NumCPU()

	if n <= 0 {
		n = np
	} else {
		n = min(n, np*100)
	}

	// the stage
	return func(g Gen[T], yield func(U) error) error {
		// context
		ctx, cancel := context.WithCancelCause(ctx)

		// waiter
		var wg sync.WaitGroup

		defer func() {
			cancel(nil)
			wg.Wait()
		}()

		// feeder
		chin := make(chan T, parallelChanCap)

		wg.Go(func() {
			e := g(func(x T) error {
				select {
				case chin <- x:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			})

			if e != nil {
				cancel(e)
			}

			// we don't want to close the channel on panic, because that would allow
			// for the reader loop to complete while the panic is still in progress
			close(chin)
		})

		// generator from chin
		gen := func(fn func(T) error) (e error) {
			for x := range chin {
				if e = fn(x); e != nil {
					break
				}
			}

			return
		}

		// output channel
		chout := make(chan U, parallelChanCap)
		count := int32(n)

		// chout sink
		sink := func(x U) error {
			select {
			case chout <- x:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}

		// worker
		worker := func() {
			if e := stage(gen, sink); e != nil {
				cancel(e)
			}

			// the last one to exit closes the channel
			if atomic.AddInt32(&count, -1) == 0 {
				close(chout)
			}
		}

		// start workers
		for range n {
			wg.Go(worker)
		}

		// reader loop
		for x := range chout {
			if e := yield(x); e != nil {
				cancel(e)
				break
			}
		}

		return context.Cause(ctx)
	}
}

// capacity of the channels inside Parallel
const parallelChanCap = 32
