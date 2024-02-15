#!/usr/bin/sh

usage() {
    echo "usage: $0 <maelstrom-binary-path>"
}

if [ -z $1 ]; then
    echo "no maelstrom binary path provided"
    usage
    return 1
elif ! test -f $1; then
    echo "maelstrom binary not found"
    usage
    return 1
fi

cargo build --release 2> /dev/null

$1 test -w echo --bin ./target/release/echo --node-count 1 --time-limit 10
