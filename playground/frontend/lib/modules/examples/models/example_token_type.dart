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

enum ExampleTokenType {
  /// A URI to load the plain content from.
  http,

  standard,
  userShared,
  ;

  static ExampleTokenType fromToken(String token) {
    if (token.startsWith(RegExp('http(s)?://'))) {
      return http;
    }

    final sdk = Sdk.tryParseExamplePath(token);
    if (sdk != null) {
      return standard;
    }

    return userShared;
  }
}
