package pump

import (
	"context"
	"errors"
	"runtime"
	"sync"
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

// capacity of the channel between pipe stages
const chanCap = 32

// pipe implementation
func pipeCtx[T any](ctx context.Context, src Gen[T], yield func(T) error) error {
	return execInCtx(ctx, func(env *pipeEnv) error {
		return readChan(startFeeder(env, src), yield)
	})
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
		return execInCtx(ctx, func(env *pipeEnv) error {
			// feeder channel
			feeder := startFeeder(env, src)

			// generator from feeder
			gen := func(fn func(T) error) error { return readChan(feeder, fn) }

			// collector channel
			collector := make(chan U, chanCap)

			// collector sink
			sink := toChan(env, collector)

			// start workers
			refCount := int32(n)

			env.wg.Add(n)

			for i := 0; i < n; i++ {
				go func() {
					defer func() {
						if atomic.AddInt32(&refCount, -1) == 0 {
							close(collector)
						}

						// avoid calling wg.Done on panic
						// see https://github.com/golang/go/issues/74702
						if p := recover(); p != nil {
							panic(p)
						}

						env.wg.Done()
					}()

					env.safe(stage(gen, sink))
				}()
			}

			// run
			return readChan(collector, yield)
		})
	}
}

// synchronisation pack for pipes
type pipeEnv struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
}

// error checking
func (env *pipeEnv) safe(err error) {
	if err != nil {
		env.cancel(err)
	}
}

// create pipe environment and run the given function in it
func execInCtx(ctx context.Context, fn func(*pipeEnv) error) error {
	// environment
	var env pipeEnv

	// context
	env.ctx, env.cancel = context.WithCancelCause(ctx)

	// cancel the context upon completion
	defer func() {
		if p := recover(); p != nil {
			env.cancel(errPanic)
			panic(p)
		}

		env.cancel(nil)
	}()

	// run
	env.safe(fn(&env))

	// wait for all threads to terminate
	env.wg.Wait()

	// all done
	return context.Cause(env.ctx)
}

// start feeder thread
func startFeeder[T any](env *pipeEnv, src Gen[T]) <-chan T {
	// feeder channel
	feeder := make(chan T, chanCap)

	// start feeder thread
	env.wg.Add(1)

	go func() {
		defer func() {
			close(feeder)

			// avoid calling wg.Done on panic
			// see https://github.com/golang/go/issues/74702
			if p := recover(); p != nil {
				panic(p)
			}

			env.wg.Done()
		}()

		env.safe(src(toChan(env, feeder)))
	}()

	return feeder
}

// yield from the channel
func readChan[T any](ch <-chan T, yield func(T) error) (err error) {
	for item := range ch {
		if err = yield(item); err != nil {
			break
		}
	}

	return
}

// construct function that yields to the channel
func toChan[T any](env *pipeEnv, ch chan<- T) func(T) error {
	return func(item T) error {
		select {
		case ch <- item:
			return nil
		case <-env.ctx.Done():
			return env.ctx.Err()
		}
	}
}

var errPanic = errors.New("pipeline panicked")
