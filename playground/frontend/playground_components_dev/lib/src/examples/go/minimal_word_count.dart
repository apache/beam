/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'package:playground_components/playground_components.dart';

import '../example_descriptor.dart';

const goMinimalWordCount = ExampleDescriptor(
  //
  'MinimalWordCount',
  contextLine1Based: 69,
  dbPath: 'SDK_GO_MinimalWordCount',
  path: '/sdks/go/examples/minimal_wordcount/minimal_wordcount.go',
  sdk: Sdk.go,

  outputContains: [
    'Reading from gs://apache-beam-samples/shakespeare/kinglear.txt',
    'Writing to wordcounts.txt',
    '''
Nodes: {1: []uint8/bytes GLO}
{2: string/string GLO}
{3: string/string GLO}
{4: KV<string,int64>/KV<string,varint> GLO}
{5: string/string GLO}
{6: string/string GLO}
{7: KV<string,int>/KV<string,int[varintz]> GLO}
{8: CoGBK<string,int>/CoGBK<string,int[varintz]> GLO}
{9: KV<string,int>/KV<string,int[varintz]> GLO}
{10: string/string GLO}
{11: KV<int,string>/KV<int[varintz],string> GLO}
{12: CoGBK<int,string>/CoGBK<int[varintz],string> GLO}
Edges: 1: Impulse [] -> [Out: []uint8 -> {1: []uint8/bytes GLO}]
2: ParDo [In(Main): []uint8 <- {1: []uint8/bytes GLO}] -> [Out: T -> {2: string/string GLO}]
3: ParDo [In(Main): string <- {2: string/string GLO}] -> [Out: string -> {3: string/string GLO}]
4: ParDo [In(Main): string <- {3: string/string GLO}] -> [Out: KV<string,int64> -> {4: KV<string,int64>/KV<string,varint> GLO}]
5: ParDo [In(Main): KV<string,int64> <- {4: KV<string,int64>/KV<string,varint> GLO}] -> [Out: string -> {5: string/string GLO}]
6: ParDo [In(Main): string <- {5: string/string GLO}] -> [Out: string -> {6: string/string GLO}]
7: ParDo [In(Main): T <- {6: string/string GLO}] -> [Out: KV<T,int> -> {7: KV<string,int>/KV<string,int[varintz]> GLO}]
8: CoGBK [In(Main): KV<string,int> <- {7: KV<string,int>/KV<string,int[varintz]> GLO}] -> [Out: CoGBK<string,int> -> {8: CoGBK<string,int>/CoGBK<string,int[varintz]> GLO}]
9: Combine [In(Main): int <- {8: CoGBK<string,int>/CoGBK<string,int[varintz]> GLO}] -> [Out: KV<string,int> -> {9: KV<string,int>/KV<string,int[varintz]> GLO}]
10: ParDo [In(Main): KV<string,int> <- {9: KV<string,int>/KV<string,int[varintz]> GLO}] -> [Out: string -> {10: string/string GLO}]
11: ParDo [In(Main): T <- {10: string/string GLO}] -> [Out: KV<int,T> -> {11: KV<int,string>/KV<int[varintz],string> GLO}]
12: CoGBK [In(Main): KV<int,string> <- {11: KV<int,string>/KV<int[varintz],string> GLO}] -> [Out: CoGBK<int,string> -> {12: CoGBK<int,string>/CoGBK<int[varintz],string> GLO}]
13: ParDo [In(Main): CoGBK<int,string> <- {12: CoGBK<int,string>/CoGBK<int[varintz],string> GLO}] -> []
'''],
);
