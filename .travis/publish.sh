#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ] && ! grep -q SNAPSHOT version.sbt; then
  gpg --fast-import .travis/sig.asc
  sbt publishSigned
fi
