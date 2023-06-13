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

import 'package:flutter/foundation.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../auth/notifier.dart';
import '../enums/snippet_type.dart';
import '../enums/unit_completion.dart';
import '../models/unit_progress.dart';
import '../repositories/client/client.dart';
import '../repositories/models/get_user_progress_response.dart';
import '../repositories/user_progress/abstract.dart';
import '../repositories/user_progress/cloud.dart';
import '../repositories/user_progress/hive.dart';
import '../state.dart';

class UnitProgressCache extends ChangeNotifier {
  final _cloudUserProgressRepository = CloudUserProgressRepository(
    client: GetIt.instance.get<TobClient>(),
  );
  final _localStorageUserProgressRepository = HiveUserProgressRepository();

  AbstractUserProgressRepository _getUserProgressRepository() {
    if (isAuthenticated) {
      return _cloudUserProgressRepository;
    }
    return _localStorageUserProgressRepository;
  }

  Future<GetUserProgressResponse?>? _future;

  var _unitProgress = <UnitProgressModel>[];
  final _unitProgressByUnitId = <String, UnitProgressModel>{};

  final _completedUnitIds = <String>{};
  final _updatingUnitIds = <String>{};

  bool get isAuthenticated =>
      GetIt.instance.get<AuthNotifier>().isAuthenticated;

  Future<void> loadUnitProgress(Sdk sdk) async {
    _future = _getUserProgressRepository().getUserProgress(sdk);
    final result = await _future;

    _unitProgressByUnitId.clear();
    if (result != null) {
      _unitProgress = result.units;
      for (final unitProgress in _unitProgress) {
        _unitProgressByUnitId[unitProgress.id] = unitProgress;
      }
    } else {
      _unitProgress = [];
    }
    notifyListeners();
  }

  List<UnitProgressModel> _getUnitProgress() {
    if (_future == null) {
      unawaited(loadUnitProgress(GetIt.instance.get<AppNotifier>().sdk));
    }
    return _unitProgress;
  }

  // Completion

  Future<void> completeUnit(String sdkId, String unitId) async {
    try {
      _addUpdatingUnitId(unitId);
      await _getUserProgressRepository().completeUnit(sdkId, unitId);
    } finally {
      await loadUnitProgress(GetIt.instance.get<AppNotifier>().sdk);
      _clearUpdatingUnitId(unitId);
    }
  }

  Set<String> getCompletedUnits() {
    _completedUnitIds.clear();
    for (final unitProgress in _getUnitProgress()) {
      if (unitProgress.isCompleted) {
        _completedUnitIds.add(unitProgress.id);
      }
    }
    return _completedUnitIds;
  }

  void _addUpdatingUnitId(String unitId) {
    _updatingUnitIds.add(unitId);
    notifyListeners();
  }

  void _clearUpdatingUnitId(String unitId) {
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

  // Snippet

  bool hasSavedSnippet(String? unitId) {
    return _unitProgressByUnitId[unitId]?.userSnippetId != null;
  }

  Future<void> saveSnippet({
    required Sdk sdk,
    required List<SnippetFile> snippetFiles,
    required SnippetType snippetType,
    required String unitId,
  }) async {
    await _getUserProgressRepository().saveUnitSnippet(
      sdk: sdk,
      snippetFiles: snippetFiles,
      snippetType: snippetType,
      unitId: unitId,
    );
  }

  Future<ExampleLoadingDescriptor> getSavedDescriptor({
    required Sdk sdk,
    required String unitId,
  }) async {
    return _getUserProgressRepository().getSavedDescriptor(
      sdk: sdk,
      unitId: unitId,
    );
  }

  Future<void> deleteUserProgress() async {
    await Future.wait([
      _localStorageUserProgressRepository.deleteUserProgress(),
      _cloudUserProgressRepository.deleteUserProgress(),
    ]);
  }
}
