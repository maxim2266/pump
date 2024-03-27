package pump

import (
	"fmt"
	"log"
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
	// pipe, then convert
	pipe := Chain2(
		Pipe,
		MapE(strconv.Atoi),
	)

	err := runConvPipe(fromArgs("0", "1", "2", "3"), pipe)

	if err != nil {
		t.Error(err)
		return
	}

	err = runConvPipe(fromArgs("0", "1", "?", "3"), pipe)

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != `strconv.Atoi: parsing "?": invalid syntax` {
		t.Errorf("unexpected error: %s", err)
		return
	}

	// convert, then pipe
	pipe = Chain2(
		MapE(strconv.Atoi),
		Pipe,
	)

	err = runConvPipe(fromArgs("0", "1", "?", "3"), pipe)

	if err == nil {
		t.Error("missing error")
		return
	}

	if err.Error() != `strconv.Atoi: parsing "?": invalid syntax` {
		t.Errorf("unexpected error: %s", err)
		return
	}
}

func TestBatch(t *testing.T) {
	const N = 3

	src := []int{0, 1, 2, 3, 4, 5, 6, 7}
	pipe := Batch[int](N, true)

	for n := 1; n < len(src); n++ {
		i := 0

		err := pipe(fromSlice(src[:n]), func(b []int) error {
			if len(b) > N || len(b) == 0 {
				return fmt.Errorf("unexpected batch size: %d instead of %d", len(b), N)
			}

			for _, x := range b {
				if x != i {
					return fmt.Errorf("unexpected value: %d instead of %d", x, i)
				}

				i++
			}

			return nil
		})

		if err != nil {
			t.Error(err)
			return
		}

		if i != n {
			t.Errorf("unexpected number of iterations: %d instead of %d", i, n)
			return
		}
	}
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

func fromArgs(args ...string) G[string] {
	return func(yield func(string) error) (err error) {
		for _, s := range args {
			if err = yield(s); err != nil {
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

func runConvPipe(src G[string], pipe S[string, int]) error {
	i := 0

	return pipe(src, func(x int) error {
		if x != i {
			return fmt.Errorf("unexpected value: %d instead of %d", x, i)
		}

		i++
		return nil
	})
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
	// generator constructor
	fromArgs := func(args ...string) G[string] {
		return func(yield func(string) error) (err error) {
			for _, s := range args {
				if err = yield(s); err != nil {
					break
				}
			}

			return
		}
	}

	// pipeline (may also be composed statically, or as a function of configuration)
	pipe := Chain4(
		Map(strings.TrimSpace),
		Filter(func(s string) bool { return len(s) > 0 }),
		MapE(strconv.Atoi),
		Pipe,
	)

	// run the pipeline
	err := pipe(fromArgs(" 123 ", " 321 ", " ", "-42"), func(x int) (e error) {
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
