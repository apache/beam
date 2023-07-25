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

import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:playground_components/src/repositories/example_repository.dart';

import 'example_repository_mock.mocks.dart';
import 'examples.dart';
import 'requests.dart';

@GenerateMocks([ExampleRepository])
MockExampleRepository getMockExampleRepository() {
  final m = MockExampleRepository();

  // stubs
  when(m.getDefaultPrecompiledObject(kRequestDefaultExampleForJava))
      .thenAnswer((_) async => exampleBasePython3);
  when(m.getDefaultPrecompiledObject(kRequestDefaultExampleForGo))
      .thenAnswer((_) async => exampleBasePython3);
  when(m.getDefaultPrecompiledObject(kRequestDefaultExampleForPython))
      .thenAnswer((_) async => exampleBasePython3);
  when(m.getDefaultPrecompiledObject(kRequestDefaultExampleForScio))
      .thenAnswer((_) async => exampleBasePython3);

  return m;
}
