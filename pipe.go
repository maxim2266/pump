package pump

import (
	"context"
	"sync"
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
func pipeCtx[T any](ctx context.Context, g Gen[T], yield func(T) error) error {
	// context
	ctx, cancel := context.WithCancelCause(ctx)

	// waiter
	var wg sync.WaitGroup

	defer func() {
		cancel(nil)
		wg.Wait()
	}()

	// feeder
	ch := make(chan T, pipeChanCap)

	wg.Go(func() {
		e := g(func(x T) error {
			select {
			case ch <- x:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		})

		if e != nil {
			cancel(e)
		}

		// we don't want to close the channel on panic, because that would allow
		// for the reader loop to complete while the panic is still in progress
		close(ch)
	})

	// reader loop
	for x := range ch {
		if e := yield(x); e != nil {
			cancel(e)
			break
		}
	}

	return context.Cause(ctx)
}

// capacity of the channel inside pipe
const pipeChanCap = 32
