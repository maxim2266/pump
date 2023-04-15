package pump

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
)

type Handle[T any] struct {
	pump func(func(T) error) error
	done atomic.Bool
}

func New[T any](fn func(func(T) error) error) *Handle[T] {
	return &Handle[T]{pump: fn}
}

func (p *Handle[T]) Run(fn func(T) error) error {
	if !p.done.CompareAndSwap(false, true) {
		return errors.New("pump: attempt to reuse a pump")
	}

	return p.pump(fn)
}

func (p *Handle[T]) WithFilter(pred func(T) bool) *Handle[T] {
	return New(func(fn func(T) error) error {
		return p.Run(func(item T) (err error) {
			if pred(item) {
				err = fn(item)
			}

			return
		})
	})
}

func (p *Handle[T]) WithPipe() *Handle[T] {
	return New(func(fn func(T) error) error {
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
			if err := fn(item); err != nil {
				cancel()
				<-errch // wait for the pump to stop
				return err
			}
		}

		return <-errch
	})
}

func Batch[T any](src *Handle[T], size int) *Handle[[]T] {
	if size < 2 || size > 1_000_000_000 {
		panic("pump: invalid batch size: " + strconv.Itoa(size))
	}

	return New(func(fn func([]T) error) error {
		batch := make([]T, 0, size)

		err := src.Run(func(item T) (err error) {
			if batch = append(batch, item); len(batch) == size {
				err = fn(batch)
				batch = batch[:0]
			}

			return
		})

		switch {
		case err != nil:
			return err
		case len(batch) > 0:
			return fn(batch)
		default:
			return nil
		}
	})
}

func Map[T, U any](src *Handle[T], conv func(T) U) *Handle[U] {
	return New(func(fn func(U) error) error {
		return src.Run(func(item T) error {
			return fn(conv(item))
		})
	})
}

func MapE[T, U any](src *Handle[T], conv func(T) (U, error)) *Handle[U] {
	return New(func(fn func(U) error) error {
		return src.Run(func(item T) (err error) {
			var v U

			if v, err = conv(item); err == nil {
				err = fn(v)
			}

			return
		})
	})
}

func Chain[T any](args ...*Handle[T]) *Handle[T] {
	switch len(args) {
	case 0:
		panic("pump: Chain() called without arguments")
	case 1:
		return args[0]
	}

	return New(func(fn func(T) error) error {
		for _, p := range args {
			if err := p.Run(fn); err != nil {
				return err
			}
		}

		return nil
	})
}
