# Distributed Key-Value Store

## Overview

This project implements a distributed key-value store.

## Supported Operations

The key-value store supports the following operations:

- `PUT <key> <value>`: Store a value for a given key
- `GET <key>`: Retrieve a value for a given key
- `SWAP <key> <new_value>`: Replace an existing value
- `DELETE <key>`: Remove a key-value pair
- `SCAN <start_key> <end_key>`: Retrieve keys and values within a range

## Getting Started

### Running the Server
```bash
cargo run --release --bin server --$(config)
```

### Running the Client
```bash
cargo run --release --bin client --$(config)
```