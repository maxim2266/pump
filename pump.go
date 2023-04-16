// Package pump provides a framework for working with callback-based iterators ("pumps").
package pump

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
)

// Handle wraps up the iterator function to make sure the function is invoked
// no more than once.
type Handle[T any] struct {
	pump func(func(T) error) error
	done atomic.Bool
}

// New constructs a new pump handle from the given iterator function.
func New[T any](fn func(func(T) error) error) *Handle[T] {
	return &Handle[T]{pump: fn}
}

// Run invokes the underlying iterator with the given callback function. An error
// is returned if the pump has already been run.
func (p *Handle[T]) Run(yield func(T) error) error {
	if !p.done.CompareAndSwap(false, true) {
		return errors.New("pump: attempt to reuse a pump")
	}

	return p.pump(yield)
}

// WithFilter creates a new pump where the data are filtered using the given predicate.
func (p *Handle[T]) WithFilter(pred func(T) bool) *Handle[T] {
	return New(func(yield func(T) error) error {
		return p.Run(func(item T) (err error) {
			if pred(item) {
				err = yield(item)
			}

			return
		})
	})
}

// WithPipe creates a new pump where the original pump runs from a dedicated goroutine,
// while the calling goroutine is only involved in user callback invocations.
func (p *Handle[T]) WithPipe() *Handle[T] {
	return New(func(yield func(T) error) error {
		queue := make(chan T, 20)
		errch := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()

		go func() {
			defer func() {
				close(queue)
				close(errch)
			}()

			err := p.Run(func(item T) error {
				select {
				case queue <- item:
					return nil
				case <-ctx.Done():
					return errors.New("pump cancelled") // just to stop the pump
				}
			})

			if err != nil && ctx.Err() == nil {
				errch <- err
			}
		}()

		for item := range queue {
			if err := yield(item); err != nil {
				cancel()
				<-errch // wait for the pump to stop
				return err
			}
		}

		return <-errch
	})
}

// Batch creates a new pump that yields its data in batches of the given size.
func Batch[T any](src *Handle[T], size int) *Handle[[]T] {
	if size < 2 || size > 1_000_000_000 {
		panic("pump: invalid batch size: " + strconv.Itoa(size))
	}

	return New(func(yield func([]T) error) error {
		batch := make([]T, 0, size)

		err := src.Run(func(item T) (err error) {
			if batch = append(batch, item); len(batch) == size {
				err = yield(batch)
				batch = batch[:0]
			}

			return
		})

		switch {
		case err != nil:
			return err
		case len(batch) > 0:
			return yield(batch)
		default:
			return nil
		}
	})
}

// Map creates a new pump that converts data from the original pump via the given function.
func Map[T, U any](src *Handle[T], conv func(T) U) *Handle[U] {
	return New(func(yield func(U) error) error {
		return src.Run(func(item T) error {
			return yield(conv(item))
		})
	})
}

// MapE is the same as pump.Map, but the mapping function may also return an error.
func MapE[T, U any](src *Handle[T], conv func(T) (U, error)) *Handle[U] {
	return New(func(yield func(U) error) error {
		return src.Run(func(item T) (err error) {
			var v U

			if v, err = conv(item); err == nil {
				err = yield(v)
			}

			return
		})
	})
}

// Chain creates a new pump that invokes the given pumps one by one, from left to right.
func Chain[T any](args ...*Handle[T]) *Handle[T] {
	switch len(args) {
	case 0:
		panic("pump: Chain() called without arguments")
	case 1:
		return args[0]
	}

	return New(func(yield func(T) error) (err error) {
		for _, p := range args {
			if err = p.Run(yield); err != nil {
				break
			}
		}

		return
	})
}
