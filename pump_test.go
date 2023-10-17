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
	pipe := Chain6(
		Map(takeIn),
		Map(strings.TrimSpace),
		Filter(func(s string) bool { return len(s) > 0 }),
		MapE(strconv.Atoi),
		Map(func(x int) int { return 2 * x }),
		Map(func(x int) int { return x + 1 }),
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
	pipe := Chain3(
		Map(strings.TrimSpace),
		Filter(func(s string) bool { return len(s) > 0 }),
		MapE(strconv.Atoi),
	)

	// run the pipeline
	err := pipe(fromArgs(" 123 ", " 321 ", " "), func(x int) (e error) {
		_, e = fmt.Println(x)
		return
	})

	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// 123
	// 321
}
