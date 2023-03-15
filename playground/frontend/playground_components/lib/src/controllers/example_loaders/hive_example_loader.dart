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

import 'dart:convert';

import 'package:hive/hive.dart';

import '../../../playground_components.dart';
import 'example_loader.dart';

// TODO(nausharipov) review: move /example_loaders into /models.
class HiveExampleLoader extends ExampleLoader {
  @override
  final HiveExampleLoadingDescriptor descriptor;

  Example? _example;
  final ExampleCache exampleCache;

  HiveExampleLoader({
    required this.descriptor,
    required this.exampleCache,
  });

  @override
  Sdk? get sdk => _example?.sdk;

  Future<Example> _getExample() async {
    try {
      final box = await Hive.openBox(descriptor.boxName);
      final Map<String, dynamic> map =
          jsonDecode(box.get(descriptor.snippetId));
      return Example.fromJson(map);
    } on Exception catch (_) {
      rethrow;
    }
  }

  @override
  Future<Example> get future async {
    return _example ?? (_example = await _getExample());
  }
}
