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

/// Describes an example with the code to be fetched from [uri].
class HttpExampleLoadingDescriptor extends ExampleLoadingDescriptor {
  @override
  final Sdk sdk;

  final Uri uri;

  @override
  String get token => uri.toString();

  const HttpExampleLoadingDescriptor({
    required this.sdk,
    required this.uri,
    super.viewOptions,
  });

  @override
  List<Object> get props => [
        sdk.id,
        uri,
        viewOptions,
      ];

  @override
  HttpExampleLoadingDescriptor copyWithoutViewOptions() =>
      HttpExampleLoadingDescriptor(
        sdk: sdk,
        uri: uri,
      );

  @override
  Map<String, dynamic> toJson() => {
        'sdk': sdk.id,
        'url': uri.toString(),
        ...viewOptions.toShortMap(),
      };

  static HttpExampleLoadingDescriptor? tryParse(Map<String, dynamic> map) {
    final urlString = map['url'];
    if (urlString == null) {
      return null;
    }

    final uri = Uri.tryParse(urlString);
    if (uri == null) {
      return null;
    }

    final sdkId = map['sdk'];
    if (sdkId == null) {
      return null;
    }

    return HttpExampleLoadingDescriptor(
      sdk: Sdk.parseOrCreate(sdkId),
      uri: uri,
      viewOptions: ExampleViewOptions.fromShortMap(map),
    );
  }

  @override
  bool get isSerializableToUrl => true;
}
