---
type: languages
title: "Go SDK Dependencies"
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

# Beam SDK for Go dependencies

Beam Go uses [Go modules](https://go.dev/blog/using-go-modules) for package management.
Compile and runtime dependencies for your Beam SDK version are listed in `go.sum` in the Beam repository.
This can be found at

```
https://raw.githubusercontent.com/apache/beam/v<VERSION_NUMBER>/sdks/go.sum
```

<p class="paragraph-wrap">Replace `&lt;VERSION_NUMBER&gt;` with the major.minor.patch version of the SDK. For example, <a href="https://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/sdks/go.sum" target="_blank" rel="noopener noreferrer">https://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/sdks/go.sum</a> will provide the dependencies for the {{< param release_latest >}} release.</p>

**Note:** To just view direct dependencies, you can view the `go.mod` file instead, direct dependencies
are listed in the initial `require` statement. This can be found at `https://raw.githubusercontent.com/apache/beam/v<VERSION_NUMBER>/sdks/go.mod`
