Limbo
=====

[![Build Status](https://travis-ci.org/spotify/limbo.svg?branch=master)](https://travis-ci.org/spotify/limbo)
[![codecov.io](https://codecov.io/github/spotify/limbo/coverage.svg?branch=master)](https://codecov.io/github/spotify/limbo?branch=master)
[![GitHub license](https://img.shields.io/github/license/spotify/limbo.svg)](./LICENSE)

## Raison d'être:

Note: Limbo is in a WIP state.

Limbo is a Scala API which allows to leverage best of different data processing frameworks by
allowing seamless transition between framework specific data structures.

## Features

- Itegration between [Scio](https://github.com/spotify/scio) and [Spark](https://github.com/apache/spark)
- Programmatic Spark job submission to a Apache YARN cluster
- Scala API for Google Dataproc cluster

## Example:

```
// Start in Scio:
val (sc, args) = ContextAndArgs(argv)
val scol = sc.parallelize(1 to 10)

// Move to Spark realm
scol.toRDD().map { rdd =>
  rdd
    .map(_ * 2)
    .saveAsTextFile(args("output"))
}
```

## Code of conduct

This project adheres to the [Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md).
By participating, you are expected to honor this code.
