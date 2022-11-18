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

import 'package:collection/collection.dart';
import 'package:http/http.dart' as http;

import '../../cache/example_cache.dart';
import '../../models/example.dart';
import '../../models/example_base.dart';
import '../../models/example_loading_descriptors/http_example_loading_descriptor.dart';
import 'example_loader.dart';

class HttpExampleLoader extends ExampleLoader {
  final HttpExampleLoadingDescriptor descriptor;

  const HttpExampleLoader({
    required this.descriptor,
    // TODO(alexeyinkin): Remove when this lands: https://github.com/dart-lang/language/issues/1813
    required ExampleCache exampleCache,
  });

  @override
  Future<Example> get future async {
    final response = await http.get(descriptor.uri);

    return Example(
      name: descriptor.uri.path.split('/').lastOrNull ?? 'HTTP Example',
      path: descriptor.uri.toString(),
      sdk: descriptor.sdk,
      source: response.body,
      type: ExampleType.example,
      viewOptions: descriptor.viewOptions,
    );
  }
}
