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

import '../example_view_options.dart';
import '../sdk.dart';
import 'example_loading_descriptor.dart';

/// Describes an example to be loaded from the catalog.
class StandardExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  /// The identifier of the example in the catalog.
  final String path;

  @override
  final Sdk sdk;

  @override
  String get token => path;

  const StandardExampleLoadingDescriptor({
    required this.path,
    required this.sdk,
    super.viewOptions,
  });

  @override
  List<Object> get props => [
        path,
        sdk.id,
        viewOptions,
      ];

  @override
  StandardExampleLoadingDescriptor copyWithoutViewOptions() =>
      StandardExampleLoadingDescriptor(
        path: path,
        sdk: sdk,
      );

  @override
  Map<String, dynamic> toJson() => {
        'path': path,
        'sdk': sdk.id,
        ...viewOptions.toShortMap(),
      };

  static StandardExampleLoadingDescriptor? tryParse(Map<String, dynamic> map) {
    final path = map['path'];
    final sdkId = map['sdk'];

    if (path == null || sdkId == null) {
      return null;
    }

    return StandardExampleLoadingDescriptor(
      path: path,
      sdk: Sdk.parseOrCreate(sdkId),
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  @override
  bool get isSerializableToUrl => true;
}
