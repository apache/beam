import 'package:flutter/material.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';

class AuthNotifier extends ChangeNotifier {
  // It is VERY important that you have HTTP Strict Forward Secrecy enabled and the proper headers applied to your responses or you could be subject to a javascript hijack.
  static const _storage = FlutterSecureStorage();
  String? _token;
  static const _tokenStorageKey = 'token';

  AuthNotifier() {
    _read();
  }

  bool get isAuthenticated => _token != null;

  Future<void> _read() async {
    _token = await _storage.read(key: _tokenStorageKey);
    notifyListeners();
  }

  Future<void> signIn() async {
    _token = 'value';
    await _storage.write(
      key: _tokenStorageKey,
      value: _token,
    );
    notifyListeners();
  }
}
