import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';

class AppNotifier extends ChangeNotifier {
  static const _storage = FlutterSecureStorage();
  static const _sdkIdStorageKey = 'sdkId';
  String? _sdkId;

  AppNotifier() {
    unawaited(_readSdkId());
  }

  String? get sdkId => _sdkId;

  set sdkId(String? newValue) {
    _sdkId = newValue;
    unawaited(_writeSdkId());
    notifyListeners();
  }

  Future<void> _writeSdkId() async {
    await _storage.write(
      key: _sdkIdStorageKey,
      value: _sdkId,
    );
  }

  Future<void> _readSdkId() async {
    _sdkId = await _storage.read(key: _sdkIdStorageKey);
    notifyListeners();
  }
}
