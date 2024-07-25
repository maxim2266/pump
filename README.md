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

#### Benchmarks
All benchmarks below simply pump integers through stages with no processing at all, thus only
measuring the overhead associated with running stages themselves. The first benchmark (the simplest
pass-through stage) shows a very small overhead probably due to compiler optimisations, but that
also highlights the fact that the iteration itself is generally quite efficient. Benchmarks
for Pipe and Parallel stages show higher overhead because of the Go channels used internally
(one channel for Pipe stage, and two for Parallel).
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
