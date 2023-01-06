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

import 'constants/storage_keys.dart';

class AppNotifier extends ChangeNotifier {
  String? _sdkId;

  AppNotifier() {
    unawaited(_readSdkId());
  }

  // TODO(nausharipov): remove sdkId getter and setter
  String? get sdkId => _sdkId;
  Sdk? get sdk => Sdk.tryParse(_sdkId);

  set sdkId(String? newValue) {
    _sdkId = newValue;
    unawaited(_writeSdkId(newValue));
    notifyListeners();
  }

  Future<void> _writeSdkId(String? value) async {
    final preferences = await SharedPreferences.getInstance();
    if (value != null) {
      await preferences.setString(StorageKeys.sdkId, value);
    } else {
      await preferences.remove(StorageKeys.sdkId);
    }
  }

  Future<void> _readSdkId() async {
    final preferences = await SharedPreferences.getInstance();
    _sdkId = preferences.getString(StorageKeys.sdkId);
    notifyListeners();
  }
}
