# Overview

wasmx is an expansion service that expands wasm compiled binary.

# Requirements

The wasm binary must export a `ProcessElement` function where its input and output types map to the
expected PCollection inputs and outputs.

See [internal/udf/add/add.go](internal/udf/add/add.go) for an example.

# Usage

Run the following command to see usage:

NOTE the `sdks` directory and not the `sdks/go` directory.

```
cd sdks
go run ./go/cmd/wasmx
```

# Run Service

Run the following command to start the service:

NOTE the `sdks` directory and not the `sdks/go` directory.

```
cd sdks
go run ./go/cmd/wasmx serve
```

You should see:

```
2024/01/04 01:14:41 starting wasmx expansion service at :8097
```