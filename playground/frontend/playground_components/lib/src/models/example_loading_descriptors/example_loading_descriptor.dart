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

import 'package:equatable/equatable.dart';

import '../example_view_options.dart';
import '../sdk.dart';

/// Describes a single example to be loaded.
abstract class ExampleLoadingDescriptor with EquatableMixin {
  const ExampleLoadingDescriptor({
    this.viewOptions = ExampleViewOptions.empty,
  });

  final ExampleViewOptions viewOptions;

  Sdk? get sdk => null;

  /// Anything to hint at the snippet: catalog path, user-shared ID, URL, etc.
  ///
  /// This can be used for analytics or other applications to distinguish
  /// snippets in most cases but not as a strictly unique identifier.
  String? get token => null;

  ExampleLoadingDescriptor copyWithoutViewOptions();

  Map<String, dynamic> toJson();

  /// Whether this descriptor can be serialized to a URL.
  ///
  /// If false, the code must be saved at the backend before sharing.
  bool get isSerializableToUrl;
}
