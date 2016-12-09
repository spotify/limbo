Limbo
=====

[![Build Status](https://travis-ci.org/ravwojdyla/limbo.svg?branch=master)](https://travis-ci.org/ravwojdyla/limbo)
[![GitHub license](https://img.shields.io/github/license/ravwojdyla/limbo.svg)](./LICENSE)

## Raison d'être:

Note: Limbo is in a WIP state.

Limbo is a Scala API which allows to leverage best of different data processing frameworks by
allowing seamless transition between framework specific data structures.

Currently Limbo provides:

 * itegration between [Scio](https://github.com/spotify/scio) and [Spark](https://github.com/apache/spark)
 * progammatic Spark job submission to Apache YARN cluster
 * Google Dataproc cluster helpers

## Example:

```
// Start in Scio:
val (sc, args) = ContextAndArgs(argv)
val scol = sc.parallelize(1 to 10)

// Move to Spark realm
val rdd = scol.toRDD().get
rdd
  .map(_ * 2)
  .saveAsTextFile(args("output"))
```
