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
)

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
		return src(func(item T) (err error) {
			var tmp U

			if tmp, err = fn(item); err == nil {
				err = yield(tmp)
			}

			return
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

// implementation of the pipe stage
func pipeCtx[T any](ctx context.Context, src G[T], yield func(T) error) error {
	// context
	ctx, cancel := context.WithCancelCause(ctx)

	// cancel context on panic
	defer func() {
		if p := recover(); p != nil {
			cancel(errors.New("pipe consumer panicked!"))
			panic(p)
		}
	}()

	// channel
	ch := make(chan T, 32)

	// start feeder thread
	go func() {
		defer close(ch)

		err := src(func(item T) error {
			select {
			case ch <- item:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		})

		if err != nil {
			cancel(err)
		}
	}()

	// read/yield loop
	for item := range ch {
		if err := yield(item); err != nil {
			cancel(err)
			yield = nop // discard remaining items
		}
	}

	return context.Cause(ctx)
}

func nop[T any](_ T) error { return nil }

// generate chain functions
//go:generate ./gen-chains chain.go
