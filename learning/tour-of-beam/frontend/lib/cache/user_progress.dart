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
import '../models/user_progress.dart';
import 'cache.dart';

class UserProgressCache extends Cache {
  final _auth = GetIt.instance.get<AuthNotifier>();
  UserProgressCache({required super.client}) {
    _auth.addListener(() {
      // TODO(nausharipov): get sdk
      updateCompletedUnits('go');
    });
  }

  final _completedUnits = <String>{};
  Future<List<UserProgressModel>?>? _future;

  Set<String> updateCompletedUnits(String sdkId) {
    _future = null;
    return getCompletedUnits(sdkId);
  }

  Set<String> getCompletedUnits(String sdkId) {
    print(['gcu', _future]);
    if (_future == null) {
      unawaited(_loadCompletedUnits(sdkId));
    }

    return _completedUnits;
  }

  Future<Set<String>> _loadCompletedUnits(String sdkId) async {
    print(['lcu']);
    _future = client.getUserProgress(sdkId);
    final result = await _future;

    _completedUnits.clear();
    if (result != null) {
      for (final unitProgress in result) {
        if (unitProgress.isCompleted) {
          _completedUnits.add(unitProgress.id);
        }
      }
    }
    notifyListeners();
    return _completedUnits;
  }
}
