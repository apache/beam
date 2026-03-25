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

/// Describes a loadable example previously saved by some user.
class UserSharedExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  @override
  final Sdk sdk;

  final String snippetId;

  @override
  String get token => snippetId;

  const UserSharedExampleLoadingDescriptor({
    required this.sdk,
    required this.snippetId,
    super.viewOptions,
  });

  @override
  List<Object> get props => [
        sdk.id,
        snippetId,
        viewOptions,
      ];

  @override
  UserSharedExampleLoadingDescriptor copyWithoutViewOptions() =>
      UserSharedExampleLoadingDescriptor(
        sdk: sdk,
        snippetId: snippetId,
      );

  @override
  Map<String, dynamic> toJson() => {
        'sdk': sdk.id,
        'shared': snippetId,
        ...viewOptions.toShortMap(),
      };

  static UserSharedExampleLoadingDescriptor? tryParse(
    Map<String, dynamic> map,
  ) {
    final sdkId = map['sdk'];
    final snippetId = map['shared'];

    if (sdkId == null || snippetId == null) {
      return null;
    }

    return UserSharedExampleLoadingDescriptor(
      sdk: Sdk.parseOrCreate(sdkId),
      snippetId: snippetId,
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  @override
  bool get isSerializableToUrl => true;
}
