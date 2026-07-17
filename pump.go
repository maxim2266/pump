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

The package also provides a basic set of functions for composing pipeline stages, as well as support
for pipelining and parallel execution.
*/
package pump

import "iter"

// generate chain functions
//go:generate ./gen-chains chain.go

/*
Gen is a generic push iterator, or a generator. When invoked with a user-provided callback, it
is expected to iterate its data source invoking the callback once per each data item. It is also
expected to stop on the first error either from the callback, or stumbled over internally. It is
up to the user to develop their own generators, because it's not possible to provide a generic
code for all possible data sources. Also, some generators can be invoked only once (for example,
those sourcing data from a socket), so please structure your code accordingly.
*/
type Gen[T any] = func(func(T) error) error

// FromSeq creates a generator from the given iterator.
func FromSeq[T any](seq iter.Seq[T]) Gen[T] {
	return func(yield func(T) error) (e error) {
		for x := range seq {
			if e = yield(x); e != nil {
				break
			}
		}

		return
	}
}

// FromSlice creates a generator that reads data from the given slice, in order.
// Typically it saves a few nanoseconds per iteration when compared to
// FromSeq(slices.Values(src)).
func FromSlice[S ~[]T, T any](src S) Gen[T] {
	return func(yield func(T) error) (e error) {
		for _, x := range src {
			if e = yield(x); e != nil {
				break
			}
		}

		return
	}
}

// FromSlicePtr is the same as [FromSlice] except that it yields pointers to the
// items of type T.
func FromSlicePtr[S ~[]T, T any](src S) Gen[*T] {
	return func(yield func(*T) error) (e error) {
		for i := range src {
			if e = yield(&src[i]); e != nil {
				break
			}
		}

		return
	}
}

// FromMap creates a generator of key/value pairs from the given map.
func FromMap[K comparable, V any](src map[K]V) Gen[Pair[K, V]] {
	return func(yield func(Pair[K, V]) error) (e error) {
		for k, v := range src {
			if e = yield(Pair[K, V]{k, v}); e != nil {
				break
			}
		}

		return
	}
}

// Pair is the type produced by [FromMap] generator.
type Pair[K, V any] struct {
	Key   K
	Value V
}

/*
Stage is a generic type (a function) representing a pipeline stage. For any given types T and U,
the function takes a generator of type Gen[T] and a callback of type func(U) error. When invoked,
it is expected to run the generator, do whatever processing it is programmed to do, also calling
the callback function once per each data item produced. Stage function is expected to stop at
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
type Stage[T, U any] = func(Gen[T], func(U) error) error

// Filter creates a stage function that filters input items according to the given predicate.
func Filter[T any](pred func(T) bool) Stage[T, T] {
	return func(g Gen[T], yield func(T) error) error {
		return g(func(x T) (e error) {
			if pred(x) {
				e = yield(x)
			}

			return
		})
	}
}

// Map creates a stage function that converts each data item via the given function.
func Map[T, U any](m func(T) U) Stage[T, U] {
	return func(g Gen[T], yield func(U) error) error {
		return g(func(x T) error {
			return yield(m(x))
		})
	}
}

// MapE creates a stage function that converts each data item via the given function,
// stopping on the first error encountered, if any.
func MapE[T, U any](m func(T) (U, error)) Stage[T, U] {
	return func(g Gen[T], yield func(U) error) error {
		return g(func(x T) (e error) {
			var y U

			if y, e = m(x); e == nil {
				e = yield(y)
			}

			return
		})
	}
}

// Unique creates a stage that yields only the items with unique keys, skipping all duplicates.
// The key for each item is defined by the given function. The returned stage is stateful, and
// it does not work correctly within [Parallel] or [ParallelCtx].
func Unique[K comparable, V any](key func(V) K) Stage[V, V] {
	return func(g Gen[V], yield func(V) error) error {
		seen := make(map[K]struct{})

		return g(func(x V) (e error) {
			k := key(x)

			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				e = yield(x)
			}

			return
		})
	}
}

// Batch creates a stage that yields batches of the given size.
func Batch[T any](size int) Stage[T, []T] {
	if size < 1 {
		panic("invalid size in pump.Batch")
	}

	return func(g Gen[T], yield func([]T) error) (err error) {
		b := make([]T, 0, size)
		f := func(x T) (e error) {
			if b = append(b, x); len(b) == size {
				if e = yield(b); e == nil {
					b = make([]T, 0, size)
				}
			}

			return
		}

		if err = g(f); err == nil && len(b) > 0 {
			err = yield(b)
		}

		return
	}
}

// Flatten is a stage function that does the inverse of [Batch]: it yields
// individual items from each input batch.
func Flatten[T any](g Gen[[]T], yield func(T) error) error {
	return g(func(b []T) (e error) {
		for _, x := range b {
			if e = yield(x); e != nil {
				break
			}
		}

		return
	})
}

// bind a stage to a generator, returning a new generator
func bind[T, U any](g Gen[T], s Stage[T, U]) Gen[U] {
	return func(yield func(U) error) error {
		return s(g, yield)
	}
}
