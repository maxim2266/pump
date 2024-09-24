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
		it := Bind(intRange(N), stage).Iter()

		for x := range it.All {
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

			n++
		}

		if it.Err != nil {
			t.Errorf("[%d] %s", i, it.Err)
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
	const N = 10000

	atoi := MapE(strconv.Atoi)
	itoa := Map(strconv.Itoa)
	even := Filter(func(x int) bool { return x&1 == 0 })

	stages := [...]Stage[int, int]{
		Chain3(
			itoa,
			Parallel(0, atoi),
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
		res = res[:0]
		it := Bind(intRange(N), stage).Iter()

		for x := range it.All {
			res = append(res, x)
		}

		if it.Err != nil {
			t.Errorf("[%d] %s", no, it.Err)
			return
		}

		if len(res) != N/2 {
			t.Errorf("[%d] unexpected result length: %d instead of %d", no, len(res), N/2)
			return
		}

		sort.Ints(res)

		for i := 0; i < len(res); i++ {
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
		Bind(genOnes(N), Parallel(0, pass)),
	}

	for i, gen := range sources {
		count := 0
		it := gen.Iter()

		for x := range it.All {
			if count += x; count == M {
				break
			}
		}

		if it.Err != nil {
			t.Errorf("[%d] unexpected error: %s", i, it.Err)
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
		for i := 0; i < n; i++ {
			if err = yield(i); err != nil {
				break
			}
		}

		return
	}
}

func genOnes(n int) Gen[int] {
	return func(yield func(int) error) (err error) {
		for i := 0; i < n; i++ {
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

	for i := 0; i < N; i++ {
		data[i] = strconv.Itoa(i)
	}

	for n, errInd := 0, rand.Int()%N; n < M; n, errInd = n+1, rand.Int()%N {
		data[errInd] = "?"

		it := Bind(FromSlice(data), stage).Iter()

		for range it.All {
			// do nothing
		}

		if it.Err == nil {
			t.Errorf("[%d] missing error", errInd)
			return
		}

		if it.Err.Error() != `strconv.Atoi: parsing "?": invalid syntax` {
			t.Errorf("[%d] unexpected error: %s", errInd, it.Err)
			return
		}

		data[errInd] = strconv.Itoa(errInd)
	}
}

func BenchmarkSimple(b *testing.B) {
	bench(b, pass)
}

func BenchmarkRangeFunc(b *testing.B) {
	buff := make([]int, b.N)

	for i := range buff {
		buff[i] = i & 1
	}

	sum := 0
	it := Bind(FromSlice(buff), pass).Iter()

	b.ResetTimer()

	for x := range it.All {
		sum += x
	}

	b.StopTimer()

	if it.Err != nil {
		b.Error(it.Err)
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
	buff := make([]int, b.N)

	for i := range buff {
		buff[i] = i & 1
	}

	sum := 0

	b.ResetTimer()

	err := stage(FromSlice(buff), func(x int) error {
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
