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

import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';
import 'package:shared_preferences/shared_preferences.dart';

import 'constants/params.dart';
import 'constants/storage_keys.dart';

class AppNotifier extends ChangeNotifier {
  Sdk? _sdk;

  AppNotifier() {
    unawaited(_readSdk());
  }

  Sdk get sdk => _sdk ?? defaultSdk;

  set sdk(Sdk newValue) {
    _sdk = newValue;
    unawaited(_writeSdk(newValue));
    notifyListeners();
  }

  Future<void> _writeSdk(Sdk value) async {
    final preferences = await SharedPreferences.getInstance();
    await preferences.setString(StorageKeys.sdkId, value.id);
  }

  Future<void> _readSdk() async {
    final preferences = await SharedPreferences.getInstance();
    _sdk = Sdk.tryParse(preferences.getString(StorageKeys.sdkId));
    notifyListeners();
  }
}
