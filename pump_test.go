package pump

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestMap(t *testing.T) {
	type testCase struct {
		in  string
		out int
	}

	data := [...]testCase{
		{"  123 ", 123*2 + 1},
		{"  321 ", 321*2 + 1},
		{"      ", 0},
	}

	takeIn := func(tc testCase) string { return tc.in }

	i := 0
	pipe := Chain7(
		Map(takeIn),
		Map(strings.TrimSpace),
		Filter(func(s string) bool { return len(s) > 0 }),
		MapE(strconv.Atoi),
		Map(func(x int) int { return 2 * x }),
		Pipe,
		increment,
	)

	err := pipe(fromSlice(data[:]), func(x int) error {
		if x != data[i].out {
			return fmt.Errorf("[%d] unexpected value: %d instead of %d", i, x, data[i].out)
		}

		i++
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if i != len(data)-1 {
		t.Errorf("unexpected number of iterations: %d instead of %d", i, len(data)-1)
		return
	}
}

func TestPipe(t *testing.T) {
	const N = 10000

	pipe := Chain3(
		Map(strconv.Itoa),
		Pipe,
		MapE(strconv.Atoi),
	)

	var errCopy error

	i := 0
	err := pipe(genInts(N), func(x int) error {
		if x != i {
			errCopy = fmt.Errorf("unexpected value: %d instead of %d", x, i)
		} else if i >= N {
			errCopy = fmt.Errorf("excessive input: %d", x)
		}

		i++
		return errCopy
	})

	if err != nil || errCopy != nil {
		if err != errCopy {
			t.Errorf("error mismatch:\nset: %s\ngot: %s", errCopy, err)
			return
		}

		t.Error(err)
		return
	}

	if i != N {
		t.Errorf("incomplete sequence: %d items instead of %d", i, N)
		return
	}
}

func TestPipeErr(t *testing.T) {
	testPipeErr(t, Chain2(MapE(strconv.Atoi), PipeCtx[int](context.Background())))
}

func TestPipeErr2(t *testing.T) {
	testPipeErr(t, Chain2(Pipe, MapE(strconv.Atoi)))
}

func TestParallel(t *testing.T) {
	const N = 10000

	src := func(yield func(int) error) (err error) {
		for i := 0; i < N; i++ {
			if err = yield(i); err != nil {
				break
			}
		}

		return
	}

	res := make([]int, 0, N/2)

	pipe := Parallel(0, Chain3(
		Map(strconv.Itoa),
		MapE(strconv.Atoi),
		Filter(func(x int) bool { return x&1 == 0 }),
	))

	err := pipe(src, func(x int) error {
		res = append(res, x)
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if len(res) != N/2 {
		t.Errorf("unexpected result length: %d instead of %d", len(res), N/2)
		return
	}

	sort.Ints(res)

	for i := 0; i < len(res); i++ {
		if res[i] != 2*i {
			t.Errorf("[%d] unexpected value: %d instead of %d", i, res[i], 2*i)
			return
		}
	}
}

func TestParallelErr(t *testing.T) {
	testPipeErr(t, Parallel(0, MapE(strconv.Atoi)))
}

func fromSlice[T any](src []T) G[T] {
	return func(yield func(T) error) (err error) {
		for _, s := range src {
			if err = yield(s); err != nil {
				break
			}
		}

		return
	}
}

func genInts(n int) G[int] {
	return func(yield func(int) error) (err error) {
		for i := 0; i < n; i++ {
			if err = yield(i); err != nil {
				break
			}
		}

		return
	}
}

func increment(src G[int], yield func(int) error) error {
	return src(func(x int) error {
		return yield(x + 1)
	})
}

func testPipeErr(t *testing.T, pipe S[string, int]) {
	const (
		N = 1000
		M = 100
	)

	data := make([]string, N)

	for i := 0; i < N; i++ {
		data[i] = strconv.Itoa(i)
	}

	for n, errInd := 0, rand.Int()%N; n < M; n, errInd = n+1, rand.Int()%N {
		data[errInd] = "?"

		err := pipe(fromSlice(data), func(x int) error { return nil })

		if err == nil {
			t.Errorf("[%d] missing error", errInd)
			return
		}

		if err.Error() != `strconv.Atoi: parsing "?": invalid syntax` {
			t.Errorf("[%d] unexpected error: %s", errInd, err)
			return
		}

		data[errInd] = strconv.Itoa(errInd)
	}
}

func BenchmarkPipe(b *testing.B) {
	buff := make([]int, b.N)

	for i := range buff {
		buff[i] = i & 1
	}

	src := func(yield func(int) error) error {
		for _, x := range buff {
			if err := yield(x); err != nil {
				return err
			}
		}

		return nil
	}

	sum := 0

	b.ResetTimer()

	err := Pipe(src, func(x int) error {
		sum += x
		return nil
	})

	b.StopTimer()

	if err != nil {
		b.Error(err)
		return
	}

	if sum != b.N/2 {
		b.Errorf("unexpected sum: %d instead of %d", sum, b.N/2)
		return
	}
}

func Example() {
	// input data
	data := []string{" 123 ", " 321 ", " ", "-42"}

	// input data generator
	src := func(yield func(string) error) (err error) {
		// call "yield" for each string from "data" array
		for _, s := range data {
			if err = yield(s); err != nil {
				break
			}
		}

		return
	}

	// pipeline (may also be composed statically, or as a function of configuration)
	pipe := Chain4(
		// trim whitespace
		Map(strings.TrimSpace),
		// allow only non-empty strings
		Filter(func(s string) bool { return len(s) > 0 }),
		// convert to integer
		MapE(strconv.Atoi),
		// run all the above in a separate thread
		Pipe,
	)

	// run the pipeline
	err := pipe(src, func(x int) (e error) {
		// just print the value
		_, e = fmt.Println(x)
		return
	})

	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// 123
	// 321
	// -42
}
