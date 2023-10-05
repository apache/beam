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
  contextLine1Based: 38,
  dbPath: 'SDK_GO_MinimalWordCount',
  path: '/sdks/go/examples/minimal_wordcount/minimal_wordcount.go',
  sdk: Sdk.go,

  outputContains: [
    'Reading from gs://apache-beam-samples/shakespeare/kinglear.txt',
    'Writing to wordcounts.txt',
  ],
);
