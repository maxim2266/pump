package pump

import (
	"context"
	"errors"
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
		t.Fatal(err)
	}

	if i != len(data)-1 {
		t.Fatalf("unexpected number of iterations: %d instead of %d", i, len(data)-1)
	}
}

func TestFromSlicePtr(t *testing.T) {
	sum := 0

	sumfn := func(p *int) error {
		sum += *p
		return nil
	}

	if err := FromSlicePtr([]int{1, 2, 3, 4, 5})(sumfn); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if sum != 15 {
		t.Fatalf("invalid sum: %d instead of 15", sum)
	}
}

func TestFromMap(t *testing.T) {
	data := map[int]int{
		0:  1,
		10: 2,
		20: 3,
		30: 4,
		40: 5,
	}

	sumK, sumV := 0, 0

	sumfn := func(p Pair[int, int]) error {
		sumK += p.Key
		sumV += p.Value
		return nil
	}

	if err := FromMap(data)(sumfn); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if sumK != 100 {
		t.Fatalf("invalid sum of keys: %d instead of 100", sumK)
	}

	if sumV != 15 {
		t.Fatalf("invalid sum of values: %d instead of 15", sumV)
	}
}

func TestUnique(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	pipe := Unique(func(x int) int { return x })
	sum := 0

	sumfn := func(x int) error {
		sum += x
		return nil
	}

	if err := pipe(FromSlice(data), sumfn); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if sum != 15 {
		t.Fatalf("invalid sum: %d instead of 15", sum)
	}
}

func TestBatch(t *testing.T) {
	src := []int{0, 1, 2, 3, 4, 5, 6}
	res := make([]int, 0, len(src))
	g := bind(FromSlice(src), Batch[int](2))

	err := g(func(s []int) error {
		if len(res) >= len(src) {
			return errors.New("unexpected iteration")
		}

		switch len(s) {
		case 0:
			return errors.New("empty chunk")
		case 1, 2:
			break
		default:
			return fmt.Errorf("invalid chunk size: %d", len(s))
		}

		res = append(res, s...)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(src, res) {
		t.Fatalf("unexpected result: %v", res)
	}
}

func TestFlatten(t *testing.T) {
	sum := 0
	pipe := Chain2(Batch[int](3), Flatten)
	err := pipe(intRange(7), func(x int) error {
		sum += x
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}

	if sum != 21 {
		t.Fatalf("unexpected sum: %d instead of 21", sum)
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

		err := stage(intRange(N), func(x int) error {
			if failed {
				panic(fmt.Sprintf("[%d] double fault", i))
			}

			if failed = n > N; failed {
				t.Fatalf("[%d] unexpected iteration", i)
			}

			if failed = x != n; failed {
				t.Fatalf("[%d] unexpected value: %d instead of %d", i, x, n)
			}

			n++
			return nil
		})

		if err != nil {
			t.Fatalf("[%d] unexpected error: %s", i, err)
		}

		if n != N {
			t.Fatalf("[%d] unexpected number of iterations: %d instead of %d", i, n, N)
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

	for no, stage := range stages {
		res, err := Collect(intRange(N), stage)

		if err != nil {
			t.Fatalf("[%d] %s", no, err)
		}

		if len(res) != N/2 {
			t.Fatalf("[%d] unexpected result length: %d instead of %d", no, len(res), N/2)
		}

		sort.Ints(res)

		for i := range len(res) {
			if res[i] != 2*i {
				t.Fatalf("[%d] unexpected value at %d: %d instead of %d", no, i, res[i], 2*i)
			}
		}
	}
}

func TestParallelErr(t *testing.T) {
	testStageErr(t, Parallel(0, MapE(strconv.Atoi)))
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
		errInd := rand.Int() % N

		data[errInd] = "?"

		err := stage(FromSlice(data), func(_ int) error {
			// do nothing
			return nil
		})

		if err == nil {
			t.Fatalf("[%d @ %d] missing error", i, errInd)
		}

		if err.Error() != `strconv.Atoi: parsing "?": invalid syntax` {
			t.Fatalf("[%d @ %d] unexpected error: %s", i, errInd, err)
		}

		data[errInd] = strconv.Itoa(errInd)
	}
}

func BenchmarkSimple(b *testing.B) {
	bench(b, pass)
}

func BenchmarkPipe(b *testing.B) {
	bench(b, Pipe)
}

func BenchmarkParallel(b *testing.B) {
	bench(b, Parallel(0, pass[int]))
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
		b.Fatal(err)
	}

	if sum != b.N/2 {
		b.Fatalf("unexpected sum: %d instead of %d", sum, b.N/2)
	}
}

func pass[T any](src Gen[T], yield func(T) error) error {
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
