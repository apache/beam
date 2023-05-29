---
type: languages
title: "Go SDK Cross Compilation"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Overview

This page contains technical details for users starting Go SDK pipelines on machines that are not using a `linux` operating system, nor an `amd64` architecture.

Go is a statically compiled language.
To execute a Go binary on a machine, it must be compiled for the matching operating system and processor architecture.
This has implications for how Go SDK pipelines execute on [workers](/documentation/glossary/#worker).

# Development: Using `go run`

When starting your in development pipeline against a remote runner, you can use `go run` from your development environment.
The Go SDK will cross-compile your pipeline for `linux-amd64`, and use that as the pipeline's worker binary.

Alternatively, some local runners support Loopback execution.
Setting the flag `--environment_type=LOOPBACK` can cause the runner to connect back to the local binary to serve as a worker.
This can simplify development and debugging by avoiding hiding log output in a container.

# Production: Overriding the Worker Binary

Go SDK pipeline binaries have a `--worker_binary` flag to set the path to the desired worker binary.
This section will teach you how to use this flag for robust Go pipelines.

In production settings, it's common to only have access to compiled artifacts.
For Go SDK pipelines, you may need to have two: one for the launching platform, and one for the worker platform.

In order to run a Go program on a specific platform, that program must be built targeting that platform's operating system, and architecture.
The Go compiler is able to cross compile to a target architecture by setting the [`$GOOS` and `$GOARCH` environment variables](https://go.dev/doc/install/source#environment) for your build.

For example, you may be launching a pipeline from an M1 Macbook, but running the jobs on a Flink cluster executing on linux VMs with amd64 processors.
In this situation, you would need to compile your pipeline binary for both `darwin-arm64` for the launching, and `linux-amd64`.

```
# Build binary for the launching platform.
# This uses the defaults for your machine, so no new environment variables are needed.
$ go build path/to/my/pipeline -o output/launcher

# Build binary for the worker platform, linux-amd64
$ GOOS=linux GOARCH=amd64 go build path/to/my/pipeline -o output/worker
```

Execute the pipeline with the `--worker_binary` flag set to the desired binary.

```
# Launch the pipeline specifying the worker binary.
$ ./output/launcher --worker_binary=output/worker --runner=flink --endpoint=... <...other flags...>
```

# SDK Containers

Apache Beam releases [SDK specific containers](/documentation/runtime/environments/) for runners to use to launch workers.
These containers provision and initialize the worker binary as appropriate for the SDK.

At present, Go SDK worker containers are only built for the `linux-amd64` platform.
See [Issue 20807](https://github.com/apache/beam/issues/20807) for the current state of ARM64 container support.

Because Go is statically compiled, there are no runtime dependencies on a specific Go version for a container.
However, depending on how your binary is built, the
The Go release used to compile your binary will be what your workers execute.
Be sure to update to a recent [Go release](https://go.dev/doc/devel/release) for best performance.

# CG0_ENABLED=0 and glibc

From Beam 2.48.0, the default cross compile sets CGO_ENABLED=0 to reduce issues with the boot container and glibc versions.
If your pipeline requires CGO to run, see the above Overriding the Worker Binary for more information on using your own built binary.

Beam uses minimal debian containers as a base.
If your binary has specific execution requirements, [Custom Containers](/documentation/runtime/environments/) can be derieved from the released
containers to satisfy them.
Use custom containers to resolve glibc mismatches, or requiring additional binaries to be available at execution time.
