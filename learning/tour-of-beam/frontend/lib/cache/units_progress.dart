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
import '../models/unit_progress.dart';
import '../repositories/models/get_user_progress_response.dart';
import '../state.dart';
import 'cache.dart';

class UnitsProgressCache extends Cache {
  UnitsProgressCache({required super.client});

  var _unitsProgress = <UnitProgressModel>[];
  Future<GetUserProgressResponse?>? _future;

  final _completedUnitIds = <String>{};
  final _updatingUnitIds = <String>{};

  final _unitSnippets = <String, String?>{};

  Future<void> updateUnitsProgress() async {
    final sdkId = GetIt.instance.get<AppNotifier>().sdkId;
    if (sdkId != null) {
      await _loadUnitsProgress(sdkId);
    }
  }

  List<UnitProgressModel> getUnitsProgress() {
    if (_future == null) {
      unawaited(updateUnitsProgress());
    }

    return _unitsProgress;
  }

  Future<void> _loadUnitsProgress(String sdkId) async {
    _future = client.getUserProgress(sdkId);
    final result = await _future;

    if (result != null) {
      _unitsProgress = result.units;
    } else {
      _unitsProgress = [];
    }
    notifyListeners();
  }

  // Completion

  Set<String> getUpdatingUnitIds() => _updatingUnitIds;

  Set<String> getCompletedUnits() {
    _completedUnitIds.clear();
    for (final unitProgress in getUnitsProgress()) {
      if (unitProgress.isCompleted) {
        _completedUnitIds.add(unitProgress.id);
      }
    }
    return _completedUnitIds;
  }

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

  bool isUnitCompleted(String? unitId) {
    return getCompletedUnits().contains(unitId);
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

  // Snippets

  Map<String, String?> getUnitSnippets() {
    _unitSnippets.clear();
    for (final unitProgress in getUnitsProgress()) {
      _unitSnippets[unitProgress.id] = unitProgress.userSnippetId;
    }
    return _unitSnippets;
  }
}
