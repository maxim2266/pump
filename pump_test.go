package pump

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"
)

func TestPipelinedPump(t *testing.T) {
	const N = 1_000_000

	count, err := run(Pipe(gen(N)))

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

	err := Pipe(gen(N)).Run(func(i int) error {
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

	count, err := run(Pipe(genWithErr(N, errors.New(MSG))))

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

	p := Filter(gen(N), func(v int) bool { return v&1 == 1 })
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
	p := gen(100)

	if err := p.Run(func(_ int) error { return nil }); err != nil {
		t.Error(err)
		return
	}

	err := p.Run(func(_ int) error { return nil })

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != "pump.Run: detected an attempt to reuse a pump" {
		t.Errorf("unexpected error message: %s", err)
		return
	}
}

func TestMap(t *testing.T) {
	const N = 100

	p := MapE(Map(gen(N), strconv.Itoa), strconv.Atoi)

	count, err := run(p)

	if err != nil {
		t.Error(err)
		return
	}

	if count != N {
		t.Errorf("unexpected final value: %d instead of %d", count, N)
		return
	}
}

func TestPMap(t *testing.T) {
	const N = 10_000

	p := PMapE(PMap(gen(N), 4, strconv.Itoa), 4, strconv.Atoi)
	res := make([]int, 0, N)

	err := p.Run(func(v int) error {
		res = append(res, v)
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if len(res) != N {
		t.Errorf("unexpected result size: %d instead of %d", len(res), N)
		return
	}

	sort.Ints(res)

	for i, v := range res {
		if i != v {
			t.Errorf("unexpected value: %d instead of %d", v, i)
			return
		}
	}
}

func TestPMapSourceError(t *testing.T) {
	const N = 100
	const MSG = "XXX"

	p := PMapE(PMap(genWithErr(N, errors.New(MSG)), 4, strconv.Itoa), 4, strconv.Atoi)

	var count int

	err := p.Run(func(_ int) error { count++; return nil })

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != MSG {
		t.Errorf("unexpected error message: %q", err)
		return
	}

	t.Log(count)
}

func TestPMapDestError(t *testing.T) {
	const N = 1000
	const MSG = "XXX"

	p := PMapE(PMap(gen(N), 4, strconv.Itoa), 4, strconv.Atoi)

	var count int

	err := p.Run(func(_ int) (err error) {
		if count++; count == 100 {
			err = errors.New(MSG)
		}

		return
	})

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != MSG {
		t.Errorf("unexpected error message: %q", err)
		return
	}

	t.Log(count)
}

func TestPMapConvError(t *testing.T) {
	const N = 1000
	const MSG = "XXX"

	var count int

	p := PMapE(PMap(gen(N), 4, strconv.Itoa), 4, func(s string) (int, error) {
		if count++; count == 100 {
			return 0, errors.New(MSG)
		}

		return strconv.Atoi(s)
	})

	var numCalls int

	err := p.Run(func(_ int) error { numCalls++; return nil })

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != MSG {
		t.Errorf("unexpected error message: %q", err)
		return
	}

	t.Log(count, numCalls)
}

func TestChain(t *testing.T) {
	const N = 100

	p := Chain(gen(N), Map(gen(N), func(v int) int { return v + N }))

	count, err := run(p)

	if err != nil {
		t.Error(err)
		return
	}

	if count != 2*N {
		t.Errorf("unexpected final value: %d instead of %d", count, 2*N)
		return
	}
}

func TestLetWhile(t *testing.T) {
	const N = 100

	p := While(gen(N), func(v int) bool { return v < N/2 })

	count, err := run(p)

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

	p := Batch(gen(N), M)
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

func run(p *H[int]) (count int, err error) {
	err = p.Run(func(v int) error {
		if v != count {
			return fmt.Errorf("unexpected parameter: %d instead of %d", v, count)
		}

		count++
		return nil
	})

	return
}

func gen(N int) *H[int] {
	return genWithErr(N, nil)
}

func genWithErr(N int, err error) *H[int] {
	return New(func(yield func(int) error) error {
		for i := 0; i < N; i++ {
			if e := yield(i); e != nil {
				return e
			}
		}

		return err
	})
}
