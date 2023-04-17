package pump

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
)

func TestPipelinedPump(t *testing.T) {
	const N = 1_000_000

	count, err := runCountingPump(countingPump(N, nil).WithPipe())

	if err != nil {
		t.Error(err)
		return
	}

	if count != N {
		t.Errorf("unexpected final value: %d instead of %d", count, N)
		return
	}
}

func TestPipelinedPumpError(t *testing.T) {
	const N = 1000

	count := 0

	err := countingPump(N, nil).WithPipe().Run(func(i int) error {
		if count >= N/2 {
			return fmt.Errorf("unexpected call with value %d", i)
		}

		if i != count {
			return fmt.Errorf("unexpected parameter: %d instead of %d", i, count)
		}

		if count++; count == N/2 {
			return fmt.Errorf("expected error: reached value %d", count)
		}

		return nil
	})

	if err == nil {
		t.Error("missing expected error")
		return
	}

	if err.Error() != fmt.Sprintf("expected error: reached value %d", N/2) {
		t.Error("unexpected error message:", err)
		return
	}

	if count != N/2 {
		t.Errorf("unexpected final value: %d instead of %d", count, N)
		return
	}
}

func TestPipelinedPumpSourceError(t *testing.T) {
	const (
		N   = 1000
		MSG = "XXX"
	)

	count, err := runCountingPump(countingPump(N, errors.New(MSG)).WithPipe())

	if err == nil {
		t.Error("missing expected error")
		return
	}

	if err.Error() != MSG {
		t.Error("unexpected error message:", err)
		return
	}

	if count != N {
		t.Errorf("unexpected final value: %d instead of %d", count, N)
		return
	}
}

func TestFilteredPump(t *testing.T) {
	const N = 1000

	p := countingPump(N, nil).WithFilter(func(v int) bool { return v&1 == 1 })
	count := 0

	err := p.Run(func(v int) error {
		if v&1 == 0 {
			return fmt.Errorf("invalid value: %d", v)
		}

		count++
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if count != N/2 {
		t.Errorf("unexpected final value: %d instead of %d", count, N/2)
		return
	}
}

func TestDoubleRun(t *testing.T) {
	p := countingPump(100, nil)

	if err := p.Run(func(_ int) error { return nil }); err != nil {
		t.Error(err)
		return
	}

	err := p.Run(func(_ int) error { return nil })

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != "pump: attempt to reuse a pump" {
		t.Errorf("unexpected error message: %s", err)
		return
	}
}

func TestMap(t *testing.T) {
	const N = 100

	p := MapE(Map(countingPump(N, nil), strconv.Itoa), strconv.Atoi)

	count, err := runCountingPump(p)

	if err != nil {
		t.Error(err)
		return
	}

	if count != N {
		t.Errorf("unexpected final value: %d instead of %d", count, N)
		return
	}
}

func TestAll(t *testing.T) {
	const N = 100

	p := Chain(countingPump(N, nil), Map(countingPump(N, nil), func(v int) int { return v + N }))

	count, err := runCountingPump(p)

	if err != nil {
		t.Error(err)
		return
	}

	if count != 2*N {
		t.Errorf("unexpected final value: %d instead of %d", count, 2*N)
		return
	}
}

func TestWhile(t *testing.T) {
	const N = 100

	p := countingPump(N, nil).While(func(v int) bool { return v < N/2 })

	count, err := runCountingPump(p)

	if err != nil {
		t.Error(err)
		return
	}

	if count != N/2 {
		t.Errorf("unexpected final value: %d instead of %d", count, N/2)
		return
	}
}

func TestBatch(t *testing.T) {
	const (
		N = 100
		M = 9
	)

	p := Batch(countingPump(N, nil), M)
	count, batches := 0, 0

	err := p.Run(func(a []int) error {
		for i, v := range a {
			if v != count {
				return fmt.Errorf("unexpected value at index %d: %d instead of %d", i, v, count)
			}

			count++
		}

		batches++
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if count != N {
		t.Errorf("unexpected final value: %d instead of %d", count, N)
		return
	}

	n := N / M

	if N%M != 0 {
		n++
	}

	if batches != n {
		t.Errorf("unexpected number of batches: %d instead of %d", batches, n)
		return
	}
}

func runCountingPump(p *Handle[int]) (count int, err error) {
	err = p.Run(func(v int) error {
		if v != count {
			return fmt.Errorf("unexpected parameter: %d instead of %d", v, count)
		}

		count++
		return nil
	})

	return
}

func countingPump(N int, err error) *Handle[int] {
	return New(func(fn func(int) error) error {
		for i := 0; i < N; i++ {
			if e := fn(i); e != nil {
				return e
			}
		}

		return err
	})
}
