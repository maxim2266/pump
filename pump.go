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
		return errors.New("pump.Run: detected an attempt to reuse a pump")
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

// LetWhile creates a new pump that passes through all the items while the given predicate
// returns 'true'.
func (p *Handle[T]) LetWhile(pred func(T) bool) *Handle[T] {
	return New(func(yield func(T) error) error {
		ok := true

		return p.Run(func(item T) (err error) {
			// here we simply swallow all the items after the predicate returns false
			if ok = ok && pred(item); ok {
				err = yield(item)
			}

			return
		})
	})
}

// WithPipe creates a new pump that runs the original pump from a dedicated goroutine,
// while the calling goroutine is only involved in user callback invocations.
func (p *Handle[T]) WithPipe() *Handle[T] {
	return New(func(yield func(T) error) error {
		feeder := newFeederCtx[T](context.Background(), 1)

		defer feeder.cancel()

		go feeder.run(p)

		return drain(feeder.queue, feeder.errChan, feeder.cancel, yield)
	})
}

// iterate the given queue, invoking the callback on each item, and with error checks
func drain[T any](
	queue <-chan T,
	errChan <-chan error,
	cancel func(),
	yield func(T) error,
) error {
	for item := range queue {
		if err := yield(item); err != nil {
			cancel()

			// wait for the pump to stop
			for <-errChan != nil {
			}

			return err
		}
	}

	return <-errChan
}

// pipe feeder
type feederHandle[T any] struct {
	ctx     context.Context
	cancel  func()
	queue   chan T
	errChan chan error
	errRef  int32
}

const chanSize = 20

// pipe feeder constructor
func newFeederCtx[T any](parent context.Context, numErrWriters int) *feederHandle[T] {
	feeder := &feederHandle[T]{
		queue:   make(chan T, chanSize),
		errChan: make(chan error, numErrWriters),
		errRef:  int32(numErrWriters),
	}

	feeder.ctx, feeder.cancel = context.WithCancel(parent)
	return feeder
}

// feed the pipe
func (feeder *feederHandle[T]) run(p *Handle[T]) {
	defer func() {
		close(feeder.queue)
		feeder.done()
	}()

	err := p.Run(func(item T) error {
		select {
		case feeder.queue <- item:
			return nil
		case <-feeder.ctx.Done():
			return feeder.ctx.Err()
		}
	})

	if err != nil {
		feeder.errChan <- err
	}
}

// feeder completion function
func (feeder *feederHandle[T]) done() {
	if atomic.AddInt32(&feeder.errRef, -1) == 0 {
		close(feeder.errChan)
	}
}

// Batch creates a new pump that yields its data in batches of the given size.
func Batch[T any](p *Handle[T], size int) *Handle[[]T] {
	if size < 2 || size > 1_000_000_000 {
		panic("pump.Batch: invalid batch size: " + strconv.Itoa(size))
	}

	return New(func(yield func([]T) error) error {
		batch := make([]T, 0, size)

		err := p.Run(func(item T) (err error) {
			if batch = append(batch, item); len(batch) == size {
				err = yield(batch)
				batch = batch[:0]
			}

			return
		})

		if err == nil && len(batch) > 0 {
			err = yield(batch)
		}

		return err
	})
}

// Map creates a new pump that converts data from the original pump via the given function.
func Map[T, U any](p *Handle[T], conv func(T) U) *Handle[U] {
	return New(func(yield func(U) error) error {
		return p.Run(func(item T) error {
			return yield(conv(item))
		})
	})
}

// PMap does the same as pump.Map(), but the conversion function is executed in parallel from
// `np` goroutines.
func PMap[T, U any](p *Handle[T], np int, conv func(T) U) *Handle[U] {
	if np < 1 || np > 10_000 {
		panic("pump.PMap: invalid number of threads: " + strconv.Itoa(np))
	}

	return New(func(yield func(U) error) error {
		feeder := newFeederCtx[T](context.Background(), 1)

		defer feeder.cancel()

		go feeder.run(p)

		queue := make(chan U, chanSize)
		queueRef := int32(np)

		for i := 0; i < np; i++ {
			go func() {
				defer func() {
					if atomic.AddInt32(&queueRef, -1) == 0 {
						close(queue)
					}
				}()

				for item := range feeder.queue {
					select {
					case queue <- conv(item):
						// ok
					case <-feeder.ctx.Done():
						break
					}
				}
			}()
		}

		return drain(queue, feeder.errChan, feeder.cancel, yield)
	})
}

// MapE is the same as pump.Map, but the mapping function may also return an error.
func MapE[T, U any](p *Handle[T], conv func(T) (U, error)) *Handle[U] {
	return New(func(yield func(U) error) error {
		return p.Run(func(item T) (err error) {
			var v U

			if v, err = conv(item); err == nil {
				err = yield(v)
			}

			return
		})
	})
}

// PMapE does the same as pump.PMap, but the mapping function may also return an error.
func PMapE[T, U any](p *Handle[T], np int, conv func(T) (U, error)) *Handle[U] {
	if np < 1 || np > 10_000 {
		panic("pump.PMapE: invalid number of threads: " + strconv.Itoa(np))
	}

	return New(func(yield func(U) error) error {
		feeder := newFeederCtx[T](context.Background(), np+1)

		defer feeder.cancel()

		go feeder.run(p)

		queue := make(chan U, chanSize)
		queueRef := int32(np)

		for i := 0; i < np; i++ {
			go func() {
				defer func() {
					if atomic.AddInt32(&queueRef, -1) == 0 {
						close(queue)
					}

					feeder.done()
				}()

				for item := range feeder.queue {
					v, err := conv(item)

					if err != nil {
						feeder.errChan <- err
						feeder.cancel()
						break
					}

					select {
					case queue <- v:
						// ok
					case <-feeder.ctx.Done():
						break
					}
				}
			}()
		}

		return drain(queue, feeder.errChan, feeder.cancel, yield)
	})
}

// Chain creates a new pump that invokes the given pumps one by one, from left to right.
func Chain[T any](args ...*Handle[T]) *Handle[T] {
	switch len(args) {
	case 0:
		panic("pump.Chain: no pumps to compose")
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
