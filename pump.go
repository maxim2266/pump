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
	"errors"
	"iter"
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

// Iter constructs a new iterator from the given generator.
func (src Gen[T]) Iter() It[T] {
	return It[T]{src: src}
}

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
It is an iterator over the given generator. Its main purpose is to provide
a function to range over using a for loop. Since the release of Go v1.23 everybody
does range-over-function, so me too. Given some type T and a generator "src" of type
Gen[T], we can then do:

	it := src.Iter()

	for item := range it.All {
		// process item
	}

	if it.Err != nil { ... }

A generator like "src" is typically constructed as some input generator bound
to a processing stage using [Bind] function.
*/
type It[T any] struct {
	Err error // error returned from the pipeline
	src Gen[T]
}

// All is the function to range over using a for loop.
func (it *It[T]) All(yield func(T) bool) {
	it.Err = it.src(func(item T) (err error) {
		if !yield(item) {
			err = ErrStop
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

// ErrStop signals early exit from range over function loop. It is not stored in
// It.Err, but within a stage function in some (probably, rare) situations it may be
// treated as a special case.
var ErrStop = errors.New("pipeline cancelled")

// FromSeq constructs a generator from the given iterator.
func FromSeq[T any](src iter.Seq[T]) Gen[T] {
	return func(yield func(T) error) (err error) {
		for item := range src {
			if err = yield(item); err != nil {
				break
			}
		}

		return
	}
}

// FromSlice constructs a generator that reads data from the given slice, in order.
// In Go v1.23 it saves a few nanoseconds per iteration when compared to
// FromSeq(slices.Values(src)).
func FromSlice[S ~[]T, T any](src S) Gen[T] {
	return func(yield func(T) error) (err error) {
		for _, item := range src {
			if err = yield(item); err != nil {
				break
			}
		}

		return
	}
}

// All constructs a generator that invokes all the given generators one after another, in order.
func All[T any](srcs ...Gen[T]) Gen[T] {
	return func(yield func(T) error) (err error) {
		for _, src := range srcs {
			if err = src(yield); err != nil {
				break
			}
		}

		return
	}
}
