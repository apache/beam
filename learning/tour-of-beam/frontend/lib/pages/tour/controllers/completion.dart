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

import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';

import '../../../auth/notifier.dart';
import '../../../cache/unit_progress.dart';
import '../../../enums/unit_completion.dart';

class UnitsCompletionController extends ChangeNotifier {
  final _completedUnitIds = <String>{};
  final _updatingUnitIds = <String>{};

  UnitsCompletionController({
    required UnitsProgressCache unitsProgressCache,
    // required AuthNotifier authNotifier,
  }) {
    unitsProgressCache.addListener(() {
      getCompletedUnits();
      notifyListeners();
    });
    // authNotifier.addListener(notifyListeners);
  }

  Set<String> getUpdatingUnitIds() => _updatingUnitIds;

  Set<String> getCompletedUnits() {
    final unitProgressCache = GetIt.instance.get<UnitsProgressCache>();

    _completedUnitIds.clear();
    for (final unitProgress in unitProgressCache.getUnitsProgress()) {
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
}
