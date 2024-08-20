/*
Package pump provides a minimalist framework for composing data processing pipelines.
The pipelines are type-safe, impose little overhead, and can be composed either statically,
or dynamically (for example, as a function of configuration). A running pipeline stops on and
returns the first error encountered.

The package defines two generic types:

  - Data generator Gen[T]: a callback-based ("push") iterator that supplies a stream of data of
    any type T, and
  - Pipeline stage Stage[T,U]: a function that invokes input generator Gen[T], does whatever processing
    it is programmed to do, and feeds the supplied callback with data items of type U.

The package also provides a basic set of functions for composing pipeline stages and binding stages
to generators, as well as support for pipelining and parallel execution.
*/
package pump

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

// generate chain functions
//go:generate ./gen-chains chain.go

/*
Gen is a generic push iterator, or a generator. When invoked with a user-provided callback, it
is expected to iterate its data source invoking the callback once per each data item. It is also
expected to stop on the first error either from the callback, or stumbled over internally. It is up
to the user to develop their own generators, because it's not possible to provide a generic code
for all possible data sources. Also, there is one caveat: some generators can be run only once
(for example, those sourcing data from a socket), so please structure your code accordingly.
*/
type Gen[T any] func(func(T) error) error

/*
Bind takes an existing generator of some type T and returns a new generator of some type U that
does T -> U conversion via the given stage function.
*/
func Bind[T, U any](src Gen[T], stage Stage[T, U]) Gen[U] {
	return func(yield func(U) error) error {
		return stage(src, yield)
	}
}

/*
Iter is an iterator over the given generator. Its main purpose is to provide
a function to range over using a for loop. Since the release of Go v1.23 everybody
does range over functions, so me too. Given some type T and a generator "gen" of type
Gen[T], we can then do:

	src := From(gen)

	for item := range src.All {
		// process item
	}

	if src.Err != nil { ... }

A generator like "gen" is typically constructed as some input generator bound
to a processing stage using Bind() function.
*/
type Iter[T any] struct {
	Err error // error returned from the pipeline
	src Gen[T]
}

// From constructs a new iterator from the given generator function.
func From[T any](src Gen[T]) Iter[T] {
	return Iter[T]{src: src}
}

// All is the function to range over using a for loop.
func (it *Iter[T]) All(yield func(T) bool) {
	it.Err = it.src(func(item T) (e error) {
		if !yield(item) {
			e = ErrStop
		}

		return
	})

	if errors.Is(it.Err, ErrStop) {
		it.Err = nil
	}
}

/*
Stage is a generic type (a function) representing a pipeline stage. For any given types T and U,
the function takes a generator of type Gen[T] and a callback of type func(U) error. When invoked,
it is expected to run the generator, do whatever processing it is programmed to do, also calling
the callback function once per each data element produced. Stage function is expected to stop at
and return the first error (if any) from either the callback, or from the iteration itself. The
signature of the function is designed to allow for full control over when and how the source
generator is invoked. For example, suppose we want to have a pipeline stage where processing of
each input item involves database queries, and we also want to establish a database connection
before the iteration, and close it afterwards. This can be achieved using the following stage
function (for some already defined types T and U):

	func process(src pump.Gen[T], yield func(U) error) error {
		conn, err := connectToDatabase()

		if err != nil {
			return err
		}

		defer conn.Close()

		return src(func(item T) error {	// this actually invokes the source generator
			// produce a result of type U
			result, err := produceResult(item, conn)

			if err != nil {
				return err
			}

			// pass the result further down the pipeline
			return yield(result)
		})
	}

For more complex use cases Sink interface can be implemented and utilised with Run/RunPipe functions.
*/
type Stage[T, U any] func(Gen[T], func(U) error) error

// Filter creates a stage function that filters input items according to the given predicate.
func Filter[T any](pred func(T) bool) Stage[T, T] {
	return func(src Gen[T], yield func(T) error) error {
		return src(func(item T) (err error) {
			if pred(item) {
				err = yield(item)
			}

			return
		})
	}
}

// Map creates a stage function that converts each data element via the given function.
func Map[T, U any](fn func(T) U) Stage[T, U] {
	return func(src Gen[T], yield func(U) error) error {
		return src(func(item T) error {
			return yield(fn(item))
		})
	}
}

// MapE creates a stage function that converts each data element via the given function,
// stopping on the first error encountered, if any.
func MapE[T, U any](fn func(T) (U, error)) Stage[T, U] {
	return func(src Gen[T], yield func(U) error) error {
		return src(func(item T) error {
			tmp, err := fn(item)

			if err != nil {
				return err
			}

			return yield(tmp)
		})
	}
}

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
// Zero or negative value of n corresponds to runtime.NumCPU().
func Parallel[T, U any](n int, stage Stage[T, U]) Stage[T, U] {
	return ParallelCtx(context.Background(), n, stage)
}

// Parallel constructs a stage function that invokes the given stage from n
// goroutines in parallel, under control of the given context. The value of n has
// the upper bound of 100 * runtime.NumCPU(). Zero or negative value of n corresponds
// to runtime.NumCPU().
func ParallelCtx[T, U any](ctx context.Context, n int, stage Stage[T, U]) Stage[T, U] {
	// ensure realistic value for n
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

// ErrStop signals early exit from range over function loop. It is not stored in
// Iter.Err, but within a stage function in some (probably, rare) situations it may be
// treated as a special case.
var ErrStop = errors.New("pipeline cancelled")
