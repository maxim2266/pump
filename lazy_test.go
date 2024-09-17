package pump

import (
	"fmt"
	"testing"
)

func TestLazy(t *testing.T) {
	testLazy(t, pairSumPipe, []int{0, 1, 3, 5, 7, 9})
}

func TestLazyAgain(t *testing.T) {
	TestLazy(t)
}

func TestLazy1(t *testing.T) {
	testLazy(t, pairSumInitPipe, []int{-1, 1, 3, 5, 7, 9})
}

func TestLazy1Again(t *testing.T) {
	TestLazy1(t)
}

func TestLazy2(t *testing.T) {
	testLazy(t, pairSumInitOffsetPipe, []int{4, 6, 8, 10, 12, 14})
}

func TestLazy2Again(t *testing.T) {
	TestLazy2(t)
}

func testLazy(t *testing.T, pipe Stage[int, int], res []int) {
	err := pipe(intRange(6), func(x int) error {
		if len(res) == 0 {
			return fmt.Errorf("unexpected call with value %d", x)
		}

		if x != res[0] {
			return fmt.Errorf("unexpected value: %d instead of %d", x, res[0])
		}

		res = res[1:]
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	if len(res) > 0 {
		t.Errorf("%d value(s) remain in result", len(res))
		return
	}
}

func pairSum(yield func(int) error) func(int) error {
	prev := 0

	return func(x int) error {
		if err := yield(x + prev); err != nil {
			return err
		}

		prev = x
		return nil
	}
}

var pairSumPipe = Lazy(pairSum)

func pairSumInit(prev int, yield func(int) error) func(int) error {
	return func(x int) error {
		if err := yield(x + prev); err != nil {
			return err
		}

		prev = x
		return nil
	}
}

var pairSumInitPipe = Lazy1(-1, pairSumInit)

func pairSumInitOffset(prev, offset int, yield func(int) error) func(int) error {
	return func(x int) error {
		if err := yield(x + prev + offset); err != nil {
			return err
		}

		prev = x
		return nil
	}
}

var pairSumInitOffsetPipe = Lazy2(-1, 5, pairSumInitOffset)
