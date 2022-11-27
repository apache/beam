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

import 'package:get_it/get_it.dart';

import '../models/user_progress.dart';
import '../state.dart';
import 'cache.dart';

class UserProgressCache extends Cache {
  UserProgressCache({required super.client});

  final _completedUnitIds = <String>{};
  Future<List<UserProgressModel>?>? _future;

  bool isUnitCompleted(String? unitId) {
    return getCompletedUnits().contains(unitId);
  }

  void updateCompletedUnits() {
    final sdkId = GetIt.instance.get<AppNotifier>().sdkId;
    if (sdkId != null) {
      unawaited(_loadCompletedUnits(sdkId));
    }
  }

  Set<String> getCompletedUnits() {
    if (_future == null) {
      updateCompletedUnits();
    }

    return _completedUnitIds;
  }

  Future<void> _loadCompletedUnits(String sdkId) async {
    _future = client.getUserProgress(sdkId);
    final result = await _future;

    _completedUnitIds.clear();
    if (result != null) {
      for (final unitProgress in result) {
        if (unitProgress.isCompleted) {
          _completedUnitIds.add(unitProgress.unitId);
        }
      }
    }

    notifyListeners();
  }
}
