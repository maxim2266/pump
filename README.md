## pump: a minimalist framework for assembling data processing pipelines.

[![GoDoc](https://godoc.org/github.com/maxim2266/pump?status.svg)](https://godoc.org/github.com/maxim2266/pump)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxim2266/pump)](https://goreportcard.com/report/github.com/maxim2266/pump)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-yellow.svg)](https://opensource.org/licenses/BSD-3-Clause)

Package `pump` provides a minimalist framework for composing data processing pipelines.
The pipelines are type-safe, impose little overhead, and can be composed either statically,
or dynamically (for example, as a function of configuration). A running pipeline stops on and
returns the first error encountered.

The package defines two generic types:

  - Data generator `Gen[T]`: a callback-based ("push") iterator that supplies a stream of data of
    any type `T`, and
  - Pipeline stage `Stage[T,U]`: a function that invokes input generator `Gen[T]`, does whatever processing
    it is programmed to do, and feeds the supplied callback with data items of type `U`.

The package also provides a basic set of functions for composing pipeline stages and binding stages
to generators, as well as support for pipelining and parallel execution.

For API details see [documentation](https://godoc.org/github.com/maxim2266/pump).

#### Concept
The library is built around two data types: generator `Gen[T any]` and stage `Stage[T,U any]`.
Generator is a function that passes data items to its argument - a callback function `func(T) error`.
This is very similar to `iter.Seq` type from Go v1.23, except that the callback function
returns `error` instead of a boolean. Any implementation of the generator function should stop
on the first error returned from the callback, or on any internal error encountered during iteration.
Here is a (simplified) example of a constructor that creates a generator iterating over the given slice:
```Go
func fromSlice[T any](src []T) Gen[T] {
    return func(yield func(T) error) error {
        for _, item := range src {
            if err := yield(item); err != nil {
                return err
            }
        }

        return nil
    }
}
```
In practice generators are more likely to source data from files, sockets, database queries, etc.

The second type, `Stage`, is a function that is expected to invoke the given generator,
process each data item of type `T` and possibly forward the results (of type `U`) to the given callback.
The `Stage` type is defined as:
```Go
type Stage[T, U any] func(Gen[T], func(U) error) error
```
Just to give a simple example, this is a stage that increments every integer from its generator:
```Go
func increment(src Gen[int], yield func(int) error) error {
    return src(func(x int) error {
        return yield(x + 1)
    })
}
```

The rest of the library is essentially about constructing and composing stages. Multiple stages
can be composed into one using `Chain*` family of functions, for example:
```Go
pipe := Chain3(increment, times2, modulo5)
```
Here we want to calculate `(2 * (x + 1)) % 5` for each integer `x`. The resulting `pipe` is a new
stage function of type `func(Gen[int], func(int) error) error`, and it can be invoked like
```Go
err := pipe(fromSlice([]int{ 1, 2, 3 }), func(x int) error {
    _, e := fmt.Println(x)
    return e
})

if err != nil { ... }
```

To assist with writing simple stage functions like `increment` the library provides
a few helpful constructors to simplify pipe definition, for example:
```Go
pipe := Chain3(
           Map(func(x int) int { return x + 1 }),
           Map(func(x int) int { return x * 2 }),
           Map(func(x int) int { return x % 5 }),
        )
```
Or, alternatively:
```Go
inrement := Map(func(x int) int { return x + 1 })
times2   := Map(func(x int) int { return x * 2 })
modulo5  := Map(func(x int) int { return x % 5 })

pipe := Chain3(increment, times2, modulo5)
```

In fact, a stage function can convert any input type `T` to any output type `U`, so the above
pipeline can be modified to produce strings instead of integers:
```Go
pipe := Chain4(
           inrement,
           times2,
           modulo5,
           Map(strconv.Itoa),
        )
```

Or the input data can be filtered to skip odd numbers:
```Go
pipe := Chain4(
           Filter(func(x int) bool { return x & 1 == 0 }),
           inrement,
           times2,
           modulo5,
        )
```

To deal with parallelisation the library provides two helpers: `Pipe` and `Parallel`.
`Pipe` runs all stages before it in a separate goroutine, for example:
```Go
pipe := Chain4(
           inrement,
           Pipe,
           times2,
           modulo5,
        )
```
When this pipeline is invoked, its generator and `increment` stage will be running
in a dedicated goroutine, while the rest will be executed in the current goroutine.

`Parallel` executes the given stage in the specified number of goroutines, in parallel.
All stages before `Parallel` are also run in a dedicated goroutine. Example:
```Go
pipe := Chain3(
           inrement,
           Parallel(5, times2),
           modulo5,
        )
```
Upon invocation of this pipeline, its generator and `increment` stage will be running
in a dedicated goroutine, the `times2` stage will be running in 5 goroutines in parallel,
and the last stage will be in the calling goroutine.

The above pipe can also be arranged to run all stages in parallel:
```Go
pipe := Parallel(5, Chain3(
           inrement,
           times2,
           modulo5,
        ))
```

_Note_: `Parallel` stage does not preserve the order of data items.

In general, pipelines can be assembled either statically (i.e., when `pipe` is literally
a static variable), or dynamically, for example, as a function of configuration. Also,
separation between processing stages and their composition often reduces the number of
modifications we have to make to the code when requirements change.

#### Benchmarks
All benchmarks below simply pump integers through stages with no processing at all, thus only
measuring the overhead associated with running stages themselves. The first benchmark (the simplest
pass-through stage) shows a very small overhead probably due to compiler optimisations, but that
also highlights the fact that the iteration itself is generally quite efficient. Benchmarks
for `Pipe` and `Parallel` stages show higher overhead because of the Go channels used internally
(one channel for `Pipe` stage, and two for `Parallel`).
```
▶ go test -bench .
goos: linux
goarch: amd64
pkg: github.com/maxim2266/pump
cpu: Intel(R) Core(TM) i5-8500T CPU @ 2.10GHz
BenchmarkSimple-6     	608652330	         1.934 ns/op
BenchmarkPipe-6       	 9804087	       120.4 ns/op
BenchmarkParallel-6   	 3579961	       341.2 ns/op
```

#### Project status
Tested on several Linux Mint versions from 21.x range. Requires Go version 1.21 or higher.
