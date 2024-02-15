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

$1 test -w broadcast --bin ./target/release/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
