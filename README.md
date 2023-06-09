# pump: callback-based iterators for Go language.

[![GoDoc](https://godoc.org/github.com/maxim2266/pump?status.svg)](https://godoc.org/github.com/maxim2266/pump)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxim2266/pump)](https://goreportcard.com/report/github.com/maxim2266/pump)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-yellow.svg)](https://opensource.org/licenses/BSD-3-Clause)

#### About
Out of the box, the Go language defines a `range` iterator that can only be applied to a limited
set of predefined type classes, while for all the other types people have to come up with their
own means of enumerating elements.  Typically, those user-defined iterators follow the `pull`
model of iteration suitable for use in `for` loops. This works well for simple objects like slices
or maps, but quickly becomes problematic as the complexity of the iteration grows. Consider,
for example, a recursive parser where the next data item may have to be returned from a deeply
recursive call: all the state of the parser before the point of return must be explicitly
saved somewhere, and quite often this is easier said than done. Alternative to that is the
`push` model of iteration, where a user-provided callback function is called once per each
data item. It's like pumping data out to the user function, hence the name of the project.
In this model of iteration there is no need to save any state explicitly, because invocation of
the callback function later returns the control back to the point of call, which makes writing
complex iterations much easier.  Also, all the code execution paths are under the full control
of the iterator function, because even a panic from the user callback can be intercepted, and
an appropriate action taken (like closing some resource handles, or removing temporary files).
The drawback of this model is the overhead of the function call that in every particular case
the compiler may or may not be able to inline, so this model is mostly suitable for complex
iterators (or Python-style generators) where such overhead is relatively small.

This project is aiming to provide a common framework for composing and enriching callback-based
iterators. Each iterator over a sequence of elements of type `T` is represented as a function of
type
```Go
func(func(T) error) error
```
This is a function that iterates ("pumps") data to the given callback function of type `func(T) error`,
stopping at the first error encountered, which in turn may either come from the iteration
itself, or from the user callback. In practice, many pumps involve some kind of I/O, hence the
`error` return type in the signature. It is assumed that every such pump may be called no more
than once, so the framework actually wraps up the iterator function in an object of type
`pump.P` that enforces the single invocation property.

The framework makes a clear distinction between constructing a pump and invoking it. Given a
pump object, it can be invoked using its `Run` method, all the other functions only
create new pumps from the existing ones:
* `Filter` creates a pump that filters its source through the given predicate.
* `While` creates a pump that takes items from its source only while the given predicate is `true`.
* `Pipe` creates a piped version of a pump where the source pump is run from a dedicated goroutine.
* `Map` converts a pump of type `T` to another pump of type `U` via a function `func(T) U` applied
	to each data item; there is also a `MapE` version that maps via function `func(T) (U, error)`.
* `PMap` and `PMapE` - parallel versions of `Map` and `MapE` respectively.
* `Batch` converts a pump to a batched version that invokes its callback with batches
	of the given size.
* `Chain` creates a pump that invokes the given pumps one after another.

Just as a little disclaimer, the framework is focussed on utility functions for the iterators
of the above signature, but the user is still responsible for developing such iterators.
To give an idea of how a pump can be created, here is a constructor of a pump iterating over
the given slice:
```Go
func SlicePump[T any](list []T) *pump.P[T] {
    return pump.New(func(yield func(T) error) error {
        for _, item := range list {
            if err := yield(item); err != nil {
                return err
            }
        }

        return nil
    })
}
```
although, as already mentioned, constructing a pump is only worth the effort where the iteration
algorithm is stateful and complex, so the above code is here for the purpose of illustration only.

For more details on each function see [documentation](https://godoc.org/github.com/maxim2266/pump).

#### Project status
Tested on Linux Mint 21.1, requires Go version 1.19 or higher.
