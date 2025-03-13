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

import 'dart:async';

import 'package:playground_components/playground_components.dart';

import '../repositories/models/get_sdks_response.dart';
import 'cache.dart';

class SdkCache extends Cache {
  SdkCache({
    required super.client,
  });

  final _sdks = <Sdk>[];
  Future<GetSdksResponse>? _future;

  List<Sdk> getSdks() {
    if (_future == null) {
      unawaited(_loadSdks());
    }

    return _sdks;
  }

  Future<List<Sdk>> _loadSdks() async {
    _future = client.getSdks();
    final result = await _future!;

    _sdks.addAll(result.sdks);
    notifyListeners();
    return _sdks;
  }
}
