## pump: a minimalist framework for assembling data processing pipelines.

[![GoDoc](https://godoc.org/github.com/maxim2266/pump?status.svg)](https://godoc.org/github.com/maxim2266/pump)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxim2266/pump)](https://goreportcard.com/report/github.com/maxim2266/pump)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-yellow.svg)](https://opensource.org/licenses/BSD-3-Clause)

Package `pump` provides a minimalist framework for composing data processing pipelines.
The pipelines are type-safe, impose little overhead, and can be composed either statically,
or dynamically (for example, as a function of configuration). A running pipeline stops on and
returns the first error encountered.

The package defines two generic types:

  - Data generator `G[T]`: a callback-based ("push") iterator that supplies a stream of data of
    any type `T`, and
  - Pipeline stage `S[T,U]`: a function that invokes input generator `G[T]`, does whatever processing
    it is programmed to do, and feeds the supplied callback with data items of type `U`.

The package also provides a basic set of functions for composing pipeline stages and binding stages
to generators, as well as a stage that runs its generator in a separate goroutine, and parallel execution
of stages.

For more details see [documentation](https://godoc.org/github.com/maxim2266/pump).

#### Project status
Tested on several Linux Mint versions from 21.x range. Requires Go version 1.21 or higher.
