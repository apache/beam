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

import 'package:flutter/widgets.dart';
import 'package:hive/hive.dart';
import 'package:playground_components/playground_components.dart';

import '../repositories/local_storage/hive.dart';

class HiveLocalStorageCache extends ChangeNotifier {
  HiveLocalStorageCache();

  var _box = {};
  Future<Box>? _future;

  bool isUnitSnippetCached(Sdk sdk, String? unitId) =>
      _getBox(sdk.id)[unitId] != null;

  Future<void> saveCode(
    String unitId,
    ContentExampleLoadingDescriptor descriptor,
  ) async {
    await HiveLocalStorage().saveCode(unitId, descriptor);
    await _loadBox(descriptor.sdk.id);
  }

  Future<ContentExampleLoadingDescriptor?> getSavedCode(
    Sdk sdk,
    String unitId,
  ) async {
    final descriptor = await HiveLocalStorage().getSavedCode(sdk, unitId);
    await _loadBox(sdk.id);
    return descriptor;
  }

  Future<void> updateBox(String sdkId) async {
    await _loadBox(sdkId);
  }

  Map _getBox(String sdkId) {
    if (_future == null) {
      unawaited(_loadBox(sdkId));
    }

    return _box;
  }

  Future<void> _loadBox(String sdkId) async {
    _future = HiveLocalStorage().getBox(sdkId);
    final result = await _future!;

    _box = result.toMap();
    notifyListeners();
  }
}
