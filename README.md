# Praetor

A [Raft consensus](https://raft.github.io/) implementation using the [actor model](https://en.wikipedia.org/wiki/Actor_model) in Python.

Intended as a standalone demo and learning exercise. Early days ...

## Run it

```
uv run -m praetor
```

Spins up 5 nodes, wires them through an in-process actor system, and runs. Everything prints to STDOUT (exciting. I know!!!)

## What works

Stable Leader election.

## How it's built

Pure Raft core, imperative actor shell.

## Status

Not quite barely functional
