package pump

import (
	"context"
	"runtime"
	"sync/atomic"
)

// Pipe is a stage function that runs its source in a separate goroutine.
func Pipe[T any](src Gen[T], yield func(T) error) error {
	return pipeCtx(context.Background(), src, yield)
}

// PipeCtx creates a stage function that runs its source in a separate goroutine.
// The lifetime of the pipe is managed via the given context.
func PipeCtx[T any](ctx context.Context) Stage[T, T] {
	return func(src Gen[T], yield func(T) error) error {
		return pipeCtx(ctx, src, yield)
	}
}

// pipe stage implementation
func pipeCtx[T any](ctx context.Context, src Gen[T], yield func(T) error) error {
	// context
	ctx, cancel := context.WithCancelCause(ctx)

	defer cancel(nil)

	// reader loop
	pumpChan(feed(ctx, cancel, src), cancel, yield)

	return context.Cause(ctx)
}

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
	return func(src Gen[T], yield func(U) error) error {
		// context
		ctx, cancel := context.WithCancelCause(ctx)

		defer cancel(nil)

		// feeder
		chin := feed(ctx, cancel, src)

		// output channel
		chout := make(chan U, chanCap)
		count := int32(n)

		// generator from channel
		gen := func(fn func(T) error) (err error) {
			for x := range chin {
				if err = fn(x); err != nil {
					break
				}
			}

			return
		}

		// channel writer
		sink := chanSink(ctx, chout)

		// workers
		for _ = range n {
			go func() {
				defer func() {
					// we don't want to close the channel on panic, because
					// that would allow for the reader loop to complete while
					// the panic is still in progress
					if p := recover(); p != nil {
						panic(p)
					}

					// the last to exit closes the channel
					if atomic.AddInt32(&count, -1) == 0 {
						close(chout)
					}
				}()

				if err := stage(gen, sink); err != nil {
					// cancelled context ignores other cancellations
					cancel(err)
					drainChan(chin)
				}
			}()
		}

		// reader loop
		pumpChan(chout, cancel, yield)

		return context.Cause(ctx)
	}
}

// start feeder and return the reader channel
func feed[T any](ctx context.Context, cancel func(error), src Gen[T]) <-chan T {
	// channel
	pipe := make(chan T, chanCap)

	// feeder
	go func() {
		defer func() {
			// we don't want to close the channel on panic, because
			// that would allow for the reader loop to complete while
			// the panic is still in progress
			if p := recover(); p != nil {
				panic(p)
			}

			close(pipe)
		}()

		if err := src(chanSink(ctx, pipe)); err != nil {
			// cancelled context ignores other cancellations
			cancel(err)
		}
	}()

	return pipe
}

// make a yield function that writes to the channel
func chanSink[T any](ctx context.Context, dest chan<- T) func(T) error {
	return func(x T) error {
		select {
		case dest <- x:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func pumpChan[T any](src <-chan T, cancel func(error), yield func(T) error) {
	// reader loop
	for x := range src {
		if err := yield(x); err != nil {
			cancel(err)
			drainChan(src)
			break
		}
	}
}

func drainChan[T any](ch <-chan T) {
	for _ = range ch {
		// do nothing
	}
}

// capacity of the channel between pipe stages
const chanCap = 32
