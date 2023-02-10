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

import '../auth/notifier.dart';
import '../enums/unit_completion.dart';
import '../repositories/models/get_user_progress_response.dart';
import '../state.dart';
import 'cache.dart';

class UnitProgressCache extends Cache {
  UnitProgressCache({required super.client});

  final _completedUnitIds = <String>{};
  final _updatingUnitIds = <String>{};
  Future<GetUserProgressResponse?>? _future;

  Set<String> getUpdatingUnitIds() => _updatingUnitIds;

  void addUpdatingUnitId(String unitId) {
    _updatingUnitIds.add(unitId);
    notifyListeners();
  }

  void clearUpdatingUnitId(String unitId) {
    _updatingUnitIds.remove(unitId);
    notifyListeners();
  }

  bool canCompleteUnit(String? unitId) {
    if (unitId == null) {
      return false;
    }
    return _getUnitCompletion(unitId) == UnitCompletion.uncompleted;
  }

  UnitCompletion _getUnitCompletion(String unitId) {
    final authNotifier = GetIt.instance.get<AuthNotifier>();
    if (!authNotifier.isAuthenticated) {
      return UnitCompletion.unauthenticated;
    }
    if (_updatingUnitIds.contains(unitId)) {
      return UnitCompletion.updating;
    }
    if (isUnitCompleted(unitId)) {
      return UnitCompletion.completed;
    }
    return UnitCompletion.uncompleted;
  }

  bool isUnitCompleted(String? unitId) {
    return getCompletedUnits().contains(unitId);
  }

  Future<void> updateCompletedUnits() async {
    final sdkId = GetIt.instance.get<AppNotifier>().sdkId;
    if (sdkId != null) {
      await _loadCompletedUnits(sdkId);
    }
  }

  Set<String> getCompletedUnits() {
    if (_future == null) {
      unawaited(updateCompletedUnits());
    }

    return _completedUnitIds;
  }

  Future<void> _loadCompletedUnits(String sdkId) async {
    _future = client.getUserProgress(sdkId);
    final result = await _future;

    _completedUnitIds.clear();
    if (result != null) {
      for (final unitProgress in result.units) {
        if (unitProgress.isCompleted) {
          _completedUnitIds.add(unitProgress.id);
        }
      }
    }

    notifyListeners();
  }
}
