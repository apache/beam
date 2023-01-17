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

import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/controllers/example_loaders/http_example_loader.dart';

import 'http_example_loader_test.mocks.dart';

// A random small file at a specific revision.
const _name = 'section-remote-info.yaml';
const _path =
    'https://raw.githubusercontent.com/apache/beam/238356dade3df54ea3d3f84a7424fa5c99bf37a4/learning/katas/go/core_transforms/$_name';

const _contents = '''
id: 131788
update_date: Mon, 27 Jul 2020 18:50:54 UTC
''';

const _sdk = Sdk.go;

@GenerateMocks([ExampleCache])
void main() {
  test('HttpExampleLoader', () async {
    final loader = HttpExampleLoader(
      descriptor: HttpExampleLoadingDescriptor(
        sdk: _sdk,
        uri: Uri.parse(_path),
      ),
      exampleCache: MockExampleCache(),
    );

    final example = await loader.future;

    // TODO(alexeyinkin): Compare whole objects when that gets to include all fields, https://github.com/apache/beam/issues/23979
    expect(example.name, _name);
    expect(example.sdk, _sdk);
    expect(example.files.first.content, _contents);
    expect(example.type, ExampleType.example);
    expect(example.path, _path);
  });
}
