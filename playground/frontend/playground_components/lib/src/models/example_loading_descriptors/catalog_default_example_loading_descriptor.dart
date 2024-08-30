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

const _key = 'default';

/// Describes a single loadable example that is default for its [sdk].
class CatalogDefaultExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  @override
  final Sdk sdk;

  const CatalogDefaultExampleLoadingDescriptor({
    required this.sdk,
    super.viewOptions,
  });

  @override
  List<Object> get props => [
        sdk.id,
        viewOptions,
      ];

  @override
  CatalogDefaultExampleLoadingDescriptor copyWithoutViewOptions() =>
      CatalogDefaultExampleLoadingDescriptor(
        sdk: sdk,
      );

  @override
  Map<String, dynamic> toJson() => {
        'sdk': sdk.id,
        _key: true,
        ...viewOptions.toShortMap(),
      };

  static CatalogDefaultExampleLoadingDescriptor? tryParse(
    Map<String, dynamic> map,
  ) {
    if (map[_key] != true) {
      return null;
    }

    return CatalogDefaultExampleLoadingDescriptor(
      sdk: Sdk.parseOrCreate(map['sdk']),
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  @override
  bool get isSerializableToUrl => true;
}
