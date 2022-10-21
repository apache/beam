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
import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'stage_enum.dart';

class AuthNotifier extends ChangeNotifier {
  // TODO(nausharipov): discuss HTTP Strict Forward Secrecy & proper headers
  // https://pub.dev/packages/flutter_secure_storage#configure-web-version
  AuthStage _authStage = AuthStage.loading;
  static const _storage = FlutterSecureStorage();
  static const _tokenStorageKey = 'token';
  String? _token;

  AuthNotifier() {
    unawaited(_read());
  }

  AuthStage get authStage => _authStage;

  Future<void> _read() async {
    _token = await _storage.read(key: _tokenStorageKey);
    if (_token == null) {
      _authStage = AuthStage.unauthenticated;
    } else {
      _authStage = AuthStage.verifying;
      await dummyDelay();
      _authStage = AuthStage.authenticated;
    }
    notifyListeners();
  }

  Future<void> signIn() async {
    if (_authStage == AuthStage.unauthenticated) {
      _token = 'value';
      await dummyDelay();
      await _storage.write(
        key: _tokenStorageKey,
        value: _token,
      );
      await _read();
      notifyListeners();
    }
  }

  Future<void> dummyDelay() async {
    await Future.delayed(const Duration(seconds: 2));
  }
}
