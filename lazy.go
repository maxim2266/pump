package pump

/*
Lazy creates a stage that is lazily initialised via the given constructor function. The
constructor is called once upon every invocation of the stage before the iteration starts. It
receives a "yield" continuation function as a parameter, and is expected to return a callback
function for the source generator. This arrangement allows for some internal state to be used
during the iteration, and the constructor can create a fresh instance of the state upon each
invocation of the stage. Examples of such internal state are item counters, flags, caches, etc.
Here is an example of a constructor of the function that deduplicates data items (of some type
Item) by their IDs:

	func dedup(yield func(*Item) error) func(*Item) error {
		// internal state: a set to detect duplicates
		seen := make(map[int]struct{})

		return func(item *Item) error {
			// check for duplicate
			if _, yes := seen[item.ID]; yes {
				log.Warn("skipped a duplicate of the item %d", item.ID)
				return nil
			}

			// mark as seen
			seen[item.ID] = struct{}{}

			// yield
			return yield(item)
		}
	}

This function can later be added as a pipeline stage, for example:

	pipe := Chain3(..., Lazy(dedup), ...)
*/
func Lazy[T, U any](create func(func(U) error) func(T) error) Stage[T, U] {
	return func(src Gen[T], yield func(U) error) error {
		return src(create(yield))
	}
}

// Lazy1 does the same as [Lazy], but with one additional parameter passed over to the constructor.
func Lazy1[A, T, U any](arg A, create func(A, func(U) error) func(T) error) Stage[T, U] {
	return func(src Gen[T], yield func(U) error) error {
		return src(create(arg, yield))
	}
}

// Lazy2 does the same as [Lazy], but with two additional parameters passed over to the constructor.
func Lazy2[A, B, T, U any](arg1 A, arg2 B, create func(A, B, func(U) error) func(T) error) Stage[T, U] {
	return func(src Gen[T], yield func(U) error) error {
		return src(create(arg1, arg2, yield))
	}
}
