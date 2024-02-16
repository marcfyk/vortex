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

if cargo build --release ; then
    $1 test -w unique-ids --bin ./target/release/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
else 
    echo "cargo build error"
    return 1
fi
