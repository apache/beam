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

import '../models/unit_content.dart';
import 'cache.dart';

class UnitContentCache extends Cache {
  UnitContentCache({
    required super.client,
  });

  final _unitContents = <String, Map<String, UnitContentModel>>{};
  final _futures = <String, Map<String, Future<UnitContentModel>>>{};

  Future<UnitContentModel> getUnitContent(
    String sdkId,
    String unitId,
  ) async {
    final future = _futures[sdkId]?[unitId];
    if (future == null || _unitContents[sdkId]?[unitId] == null) {
      await _loadUnitContent(sdkId, unitId);
    }

    return _unitContents[sdkId]![unitId]!;
  }

  Future<UnitContentModel> _loadUnitContent(String sdkId, String unitId) async {
    final future = client.getUnitContent(sdkId, unitId);
    if (!_futures.containsKey(sdkId)) {
      _futures[sdkId] = {};
    }
    _futures[sdkId]![unitId] = future;

    final result = await future;
    if (!_unitContents.containsKey(sdkId)) {
      _unitContents[sdkId] = {};
    }
    _unitContents[sdkId]![unitId] = result;

    notifyListeners();
    return result;
  }
}
