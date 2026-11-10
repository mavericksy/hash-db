#!/bin/sh
# Run the hash-db test suite. Print only the test summary and failures.
cd "$(dirname "$0")/.." || exit 1
sbcl --noinform --non-interactive \
  --eval '(push (truename ".") asdf:*central-registry*)' \
  --eval '(asdf:load-system :hash-db :force t)' \
  --eval '(asdf:test-system :hash-db)' 2>&1 \
  | grep -vE '^;|cl-store-tests|^ Running test|^$|^Backtrace|^[0-9]+: '
