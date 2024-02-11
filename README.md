# Vortex

Solutions for [Fly.io's distributed systems challenges](https://fly.io/dist-sys/)
that run on [Maelstrom](https://github.com/jepsen-io/maelstrom).


The crate builds different binaries for each challenge, 
with the common maelstrom functionality shared between binaries in the crate library.


Given that you have the maelstrom binary installed on your local machine,
you can run the challenges' tests by running the shell scripts located at
`scripts/<challenge-name>`.
Running `./scripts/<challenge-name> <maelstrom-binary-path>` will build
the Rust binaries and run the appropriate test using maelstrom.
