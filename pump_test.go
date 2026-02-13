package pump

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestChain(t *testing.T) {
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

	err := pipe(FromSeq(slices.Values(data[:])), func(x int) error {
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

	atoi := MapE(strconv.Atoi)
	itoa := Map(strconv.Itoa)

	stages := [...]Stage[int, int]{
		Pipe[int],

		Chain3(
			itoa,
			Pipe,
			atoi,
		),

		Chain3(
			Pipe,
			itoa,
			atoi,
		),

		Chain3(
			itoa,
			atoi,
			Pipe,
		),
	}

	for i, stage := range stages {
		failed := false
		n := 0

		var err error

		for x := range Bind(intRange(N), stage).All(&err) {
			if failed {
				panic(fmt.Sprintf("[%d] double fault", i))
			}

			if failed = n > N; failed {
				t.Errorf("[%d] unexpected iteration", i)
				return
			}

			if failed = x != n; failed {
				t.Errorf("[%d] unexpected value: %d instead of %d", i, x, n)
				return
			}

			if failed = err != nil; failed {
				t.Errorf("[%d] unexpected error (1): %s", i, err)
				return
			}

			n++
		}

		if err != nil {
			t.Errorf("[%d] unexpected error (2): %s", i, err)
			return
		}

		if n != N {
			t.Errorf("[%d] unexpected number of iterations: %d instead of %d", i, n, N)
			return
		}
	}
}

func TestPipeErr(t *testing.T) {
	testStageErr(t, Chain2(MapE(strconv.Atoi), PipeCtx[int](context.Background())))
}

func TestPipeErr2(t *testing.T) {
	testStageErr(t, Chain2(Pipe, MapE(strconv.Atoi)))
}

func TestParallel(t *testing.T) {
	const N = 3000

	atoi := MapE(strconv.Atoi)
	itoa := Map(strconv.Itoa)
	even := Filter(func(x int) bool { return x&1 == 0 })

	stages := [...]Stage[int, int]{
		Chain3(
			itoa,
			Parallel(0, atoi),
			even,
		),

		Chain4(
			itoa,
			Parallel(0, atoi),
			even,
			delay,
		),

		Chain3(
			itoa,
			Parallel(0, Chain2(delay, atoi)),
			even,
		),

		Parallel(0,
			Chain3(
				itoa,
				atoi,
				even,
			),
		),

		Chain2(
			itoa,
			Parallel(0,
				Chain2(
					atoi,
					even,
				),
			),
		),

		Chain2(
			Parallel(0,
				Chain2(
					itoa,
					atoi,
				),
			),
			even,
		),
	}

	res := make([]int, 0, N/2)

	for no, stage := range stages {
		var err error

		res = res[:0]

		for x := range Bind(intRange(N), stage).All(&err) {
			res = append(res, x)
		}

		if err != nil {
			t.Errorf("[%d] %s", no, err)
			return
		}

		if len(res) != N/2 {
			t.Errorf("[%d] unexpected result length: %d instead of %d", no, len(res), N/2)
			return
		}

		sort.Ints(res)

		for i := range len(res) {
			if res[i] != 2*i {
				t.Errorf("[%d] unexpected value at %d: %d instead of %d", no, i, res[i], 2*i)
				return
			}
		}
	}
}

func TestParallelErr(t *testing.T) {
	testStageErr(t, Parallel(0, MapE(strconv.Atoi)))
}

func TestEarlyExit(t *testing.T) {
	const (
		N = 50
		M = 20
	)

	sources := [...]Gen[int]{
		genOnes(N),
		Bind(genOnes(N), Pipe),
		Bind(genOnes(N), Chain2(Pipe, delay[int])),
		Bind(genOnes(N), Parallel(0, pass)),
		Bind(genOnes(N), Parallel(0, delay[int])),
	}

	for i, gen := range sources {
		var err error

		count := 0

		for x := range gen.All(&err) {
			if count += x; count == M {
				break
			}
		}

		if err != nil {
			t.Errorf("[%d] unexpected error: %s", i, err)
			return
		}

		if count != M {
			t.Errorf("[%d] unexpected value: %d instead of %d", i, count, M)
			return
		}
	}
}

func TestAll(t *testing.T) {
	s1, s2 := [...]int{0, 1, 2}, [...]int{3, 4, 5}
	count, n := 0, len(s1)+len(s2)

	err := All(FromSlice(s1[:]), FromSlice(s2[:]))(func(x int) error {
		if x != count {
			return fmt.Errorf("unexpected value: %d instead of %d", x, count)
		}

		if count++; count > n {
			return fmt.Errorf("unexpected call with value %d", x)
		}

		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if count != n {
		t.Errorf("unexpected number of calls: %d instead of %d", count, n)
		return
	}
}

func intRange(n int) Gen[int] {
	return func(yield func(int) error) (err error) {
		for i := range n {
			if err = yield(i); err != nil {
				break
			}
		}

		return
	}
}

func genOnes(n int) Gen[int] {
	return func(yield func(int) error) (err error) {
		for range n {
			if err = yield(1); err != nil {
				break
			}
		}

		return
	}
}

func increment(src Gen[int], yield func(int) error) error {
	return src(func(x int) error {
		return yield(x + 1)
	})
}

func testStageErr(t *testing.T, stage Stage[string, int]) {
	const (
		N = 1000
		M = 100
	)

	data := make([]string, N)

	for i := range N {
		data[i] = strconv.Itoa(i)
	}

	for i := range M {
		var err error

		errInd := rand.Int() % N

		data[errInd] = "?"

		for range Bind(FromSlice(data), stage).All(&err) {
			// do nothing
		}

		if err == nil {
			t.Errorf("[%d @ %d] missing error", i, errInd)
			return
		}

		if err.Error() != `strconv.Atoi: parsing "?": invalid syntax` {
			t.Errorf("[%d @ %d] unexpected error: %s", i, errInd, err)
			return
		}

		data[errInd] = strconv.Itoa(errInd)
	}
}

func BenchmarkSimple(b *testing.B) {
	bench(b, pass)
}

func BenchmarkRangeFunc(b *testing.B) {
	gen := func(yield func(int) error) (err error) {
		for i := range b.N {
			if err = yield(i & 1); err != nil {
				break
			}
		}

		return
	}

	sum := 0

	var err error

	b.ResetTimer()

	for x := range Bind(gen, pass).All(&err) {
		sum += x
	}

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

func BenchmarkPipe(b *testing.B) {
	bench(b, Pipe)
}

func BenchmarkParallel(b *testing.B) {
	bench(b, Parallel(0, pass))
}

func bench(b *testing.B, stage Stage[int, int]) {
	gen := func(yield func(int) error) (err error) {
		for i := range b.N {
			if err = yield(i & 1); err != nil {
				break
			}
		}

		return
	}

	sum := 0

	b.ResetTimer()

	err := stage(gen, func(x int) error {
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

func pass(src Gen[int], yield func(int) error) error {
	return src(yield)
}

func delay[T any](src Gen[T], yield func(T) error) error {
	return src(func(x T) error {
		time.Sleep(time.Millisecond)
		return yield(x)
	})
}

func Example() {
	// input data
	data := []string{" 123 ", " 321 ", " ", "-42"}

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
	err := pipe(FromSlice(data), func(x int) (e error) {
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
