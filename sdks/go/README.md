<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Go SDK (experimental)

The Go SDK is currently an experimental feature of Apache Beam and
not suitable for production use. It is based on the following initial 
[design](https://s.apache.org/beam-go-sdk-design-rfc).

## How to run the examples

**Prerequisites**: to use Google Cloud sources and sinks (default for
most examples), follow the setup
[here](https://beam.apache.org/documentation/runners/dataflow/). You can
verify that it works by running the corresponding Java example.

The examples are normal Go programs and are most easily run directly. They
are parameterized by Go flags. For example, to run wordcount do:

```
$ pwd
[...]/sdks/go
$ go run examples/wordcount/wordcount.go --output=/tmp/result.txt
2017/12/05 10:46:37 Pipeline:
2017/12/05 10:46:37 Nodes: {1: W<[]uint8>/GW/W<bytes>!GW}
{2: W<string>/GW/W<bytes>!GW}
{3: W<string>/GW/W<bytes>!GW}
{4: W<string>/GW/W<bytes>!GW}
{5: W<string>/GW/W<bytes>!GW}
{6: W<KV<string,int>>/GW/W<KV<bytes,int[json]>>!GW}
{7: W<GBK<string,int>>/GW/W<GBK<bytes,int[json]>>!GW}
{8: W<KV<string,int>>/GW/W<KV<bytes,int[json]>>!GW}
{9: W<string>/GW/W<bytes>!GW}
Edges: 1: Impulse [] -> [Out: W<[]uint8> -> {1: W<[]uint8>/GW/W<bytes>!GW}]
2: ParDo [In(Main): W<[]uint8> <- {1: W<[]uint8>/GW/W<bytes>!GW}] -> [Out: W<T> -> {2: W<string>/GW/W<bytes>!GW}]
3: ParDo [In(Main): W<string> <- {2: W<string>/GW/W<bytes>!GW}] -> [Out: W<string> -> {3: W<string>/GW/W<bytes>!GW}]
4: ParDo [In(Main): W<string> <- {3: W<string>/GW/W<bytes>!GW}] -> [Out: W<string> -> {4: W<string>/GW/W<bytes>!GW}]
5: ParDo [In(Main): W<string> <- {4: W<string>/GW/W<bytes>!GW}] -> [Out: W<string> -> {5: W<string>/GW/W<bytes>!GW}]
6: ParDo [In(Main): W<T> <- {5: W<string>/GW/W<bytes>!GW}] -> [Out: W<KV<T,int>> -> {6: W<KV<string,int>>/GW/W<KV<bytes,int[json]>>!GW}]
7: GBK [In(Main): KV<T,U> <- {6: W<KV<string,int>>/GW/W<KV<bytes,int[json]>>!GW}] -> [Out: GBK<T,U> -> {7: W<GBK<string,int>>/GW/W<GBK<bytes,int[json]>>!GW}]
8: Combine [In(Main): W<int> <- {7: W<GBK<string,int>>/GW/W<GBK<bytes,int[json]>>!GW}] -> [Out: W<KV<string,int>> -> {8: W<KV<string,int>>/GW/W<KV<bytes,int[json]>>!GW}]
9: ParDo [In(Main): W<KV<string,int>> <- {8: W<KV<string,int>>/GW/W<KV<bytes,int[json]>>!GW}] -> [Out: W<string> -> {9: W<string>/GW/W<bytes>!GW}]
10: ParDo [In(Main): W<string> <- {9: W<string>/GW/W<bytes>!GW}] -> []
2017/12/05 10:46:37 Execution units:
2017/12/05 10:46:37 1: Impulse[0]
2017/12/05 10:46:37 2: ParDo[beam.createFn] Out:[3]
2017/12/05 10:46:37 3: ParDo[textio.expandFn] Out:[4]
2017/12/05 10:46:37 4: ParDo[textio.readFn] Out:[5]
2017/12/05 10:46:37 5: ParDo[main.extractFn] Out:[6]
2017/12/05 10:46:37 6: ParDo[stats.mapFn] Out:[7]
2017/12/05 10:46:37 7: GBK. Out:8
2017/12/05 10:46:37 8: Combine[stats.sumIntFn] Keyed:true (Use:false) Out:[9]
2017/12/05 10:46:37 9: ParDo[main.formatFn] Out:[10]
2017/12/05 10:46:37 10: ParDo[textio.writeFileFn] Out:[]
2017/12/05 10:46:37 Reading from gs://apache-beam-samples/shakespeare/kinglear.txt
2017/12/05 10:46:38 Writing to /tmp/result.txt
```

The debugging output is currently quite verbose and likely to change. The output is a local
file in this case:

```
$ head /tmp/result.txt 
while: 2
darkling: 1
rail'd: 1
ford: 1
bleed's: 1
hath: 52
Remain: 1
disclaim: 1
sentence: 1
purse: 6
```

See [BUILD.md](./BUILD.md) for how to build Go code in general. See
[CONTAINERS.md](../CONTAINERS.md) for how to build and push the Go
SDK harness container image.

## Issues

Please use the `sdk-go` component for any bugs or feature requests.