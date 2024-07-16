/*
Package pump provides a minimalist framework for composing data processing pipelines.
The pipelines are type-safe, impose little overhead, and can be composed either statically,
or dynamically (for example, as a function of configuration).

The package defines two generic types:

  - Data generator G[T]: a callback-based ("push") iterator that supplies a stream of data of
    any type T, and
  - Pipeline stage S[T,U]: a function that invokes input generator G[T], does whatever processing
    it is programmed to do, and feeds the supplied callback with data items of type U.

The package also provides a basic set of functions for composing pipeline stages and binding stages
to generators, as well as a stage that runs its generator in a separate goroutine.
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
G is a generic push iterator, or a generator. When invoked with a user-provided callback, it
is expected to iterate its data source invoking the callback once per each data item. It is also
expected to stop on the first error either from the callback, or stumbled over internally. It is up
to the user to develop their own generators, because it's not possible to provide a generic code
for all possible data sources. Also, there is one caveat: some generators can be run only once
(for example, those sourcing the data from a socket), so please structure your code accordingly.
*/
type G[T any] func(func(T) error) error

/*
Bind takes an existing generator of some type T and returns a new generator of some type U that
does T -> U conversion via the given stage function.
*/
func Bind[T, U any](src G[T], fn S[T, U]) G[U] {
	return func(yield func(U) error) error {
		return fn(src, yield)
	}
}

/*
S is a generic type (a function) representing pipeline stage. The function takes a generator
of type G[T] and a callback of type func(U) error. When invoked, it is expected to run the
generator, do whatever processing it is programmed to do, also calling the callback function
once per each data element produced. Stage function is also expected to stop at and return the
first error (if any) from either the callback, or from the iteration itself. The signature of
the function is designed to allow for full control over when and how the source generator is
invoked. For example, suppose we want to have a pipeline stage where processing of each input
item involves database queries, and we also want to establish a database connection before the
iteration, and close it afterwards. This can be achieved using the following stage function
(for some already defined types T and U):

	func process(src pump.G[T], yield func(U) error) error {
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
*/
type S[T, U any] func(G[T], func(U) error) error

// Filter creates a stage function that filters input items according to the given predicate.
func Filter[T any](pred func(T) bool) S[T, T] {
	return func(src G[T], yield func(T) error) error {
		return src(func(item T) (err error) {
			if pred(item) {
				err = yield(item)
			}

			return
		})
	}
}

// Map creates a stage function that converts each data element via the given function.
func Map[T, U any](fn func(T) U) S[T, U] {
	return func(src G[T], yield func(U) error) error {
		return src(func(item T) error {
			return yield(fn(item))
		})
	}
}

// MapE creates a stage function that converts each data element via the given function,
// stopping on the first error encountered, if any.
func MapE[T, U any](fn func(T) (U, error)) S[T, U] {
	return func(src G[T], yield func(U) error) error {
		return src(func(item T) error {
			tmp, err := fn(item)

			if err != nil {
				return err
			}

			return yield(tmp)
		})
	}
}

// Pipe is a stage function that runs its source in a separate goroutine
func Pipe[T any](src G[T], yield func(T) error) error {
	return pipeCtx(context.Background(), src, yield)
}

// PipeCtx creates a stage function that runs its source in a separate goroutine.
// The lifetime of the pipe is managed via the given context.
func PipeCtx[T any](ctx context.Context) S[T, T] {
	return func(src G[T], yield func(T) error) error {
		return pipeCtx(ctx, src, yield)
	}
}

// capacity of the channel between pipe stages
const chanCap = 32

// pipe implementation
func pipeCtx[T any](ctx context.Context, src G[T], yield func(T) error) error {
	return execInCtx(ctx, func(env *pipeEnv) error {
		return readChan(startFeeder(env, src), yield)
	})
}

// Parallel constructs a stage function that invokes the given stage from n
// goroutines in parallel. The value of n has the upper bound of 100 * runtime.NumCPU().
// Zero or negative value of n corresponds to runtime.NumCPU().
func Parallel[T, U any](n int, stage S[T, U]) S[T, U] {
	return ParallelCtx(context.Background(), n, stage)
}

// Parallel constructs a stage function that invokes the given stage from n
// goroutines in parallel, under control of the given context. The value of n has
// the upper bound of 100 * runtime.NumCPU(). Zero or negative value of n corresponds
// to runtime.NumCPU().
func ParallelCtx[T, U any](ctx context.Context, n int, stage S[T, U]) S[T, U] {
	// ensure realistic value for n
	np := runtime.NumCPU()

	if n <= 0 {
		n = np
	} else {
		n = min(n, np*100)
	}

	// the stage
	return func(src G[T], yield func(U) error) error {
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
func startFeeder[T any](env *pipeEnv, src G[T]) <-chan T {
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
			return errStop
		}
	}
}

var (
	// predefined errors
	errPanic = errors.New("pipe stage panicked")
	errStop  = errors.New("pipe stage cancelled")
)

// Sink is an interface to a generic pipe terminator. When running a pipe
// via Run() or RunPipe(), the method Do() will be called on each data item,
// and in the end the Close() method will be invoked with a flag indicating successful
// completion. In practice this interface is probably worth implementing only in complex
// scenarios, for example, if we want to start a database transaction in the constructor
// of the object implementing this inteface, write the data items to the database in Do() method,
// and then in Close() method either commit the transaction on success, or roll it back on failure.
// In most other cases it may be easier to invoke a pipeline function directly with "yield"
// function defined inline.
type Sink[T any] interface {
	Do(T) error
	Close(ok bool) error
}

// RunPipe invokes Run with the pipe bound to the source.
func RunPipe[T, U any](src G[T], pipe S[T, U], makeSink func() (Sink[U], error)) error {
	return Run(Bind(src, pipe), makeSink)
}

// Run runs the given source into the sink produced by the given Sink
// constructor function.
func Run[T any](src G[T], makeSink func() (Sink[T], error)) (err error) {
	var sink Sink[T]

	if sink, err = makeSink(); err != nil {
		return
	}

	defer func() {
		if p := recover(); p != nil {
			sink.Close(false)
			panic(p)
		}

		if err == nil {
			err = sink.Close(true)
		} else {
			sink.Close(false)
		}
	}()

	return src(sink.Do)
}

// SinkInto is a convenience function that creates a Sink constructor from an existing
// sink object.
func SinkInto[T any, S Sink[T]](sink S) func() (Sink[T], error) {
	return func() (Sink[T], error) {
		return sink, nil
	}
}
