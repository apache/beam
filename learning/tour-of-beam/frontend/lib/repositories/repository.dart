import 'dart:convert' as convert;

import 'package:flutter/foundation.dart' show mustCallSuper;
import 'package:http/http.dart' as http;

import '../config.dart';
import 'exception.dart';

class APIRepository {
  static const String _authority = cloudFunctionsBaseUrl;

  final String _basePath;
  final Map<String, String> _defaultHeaders;

  APIRepository({
    required String basePath,
    Map<String, String> defaultHeaders = const {},
  })  : _basePath = basePath,
        _defaultHeaders = Map.unmodifiable(defaultHeaders);

  // A bit of wrapping for specifying mock client in tests.
  http.Client _createClient() => http.Client();

  Future<T> _withClient<T>(Future<T> Function(http.Client) fn) async {
    final client = _createClient();
    try {
      return await fn(client);
    } finally {
      client.close();
    }
  }

  /// Getting headers might be asynchronous, because a token
  /// might need to be refreshed.
  @mustCallSuper
  Future<Map<String, String>> get headers async {
    final headers = {'Content-Type': 'application/json; charset=utf-8'};
    return headers..addAll(_defaultHeaders);
  }

  /// Sends a low-level get request.
  Future<dynamic> get(
    String path, {
    Map<String, dynamic>? queryParams,
    Map<String, String> extraHeaders = const {},
  }) async {
    final uri = Uri.https(_authority, _basePath + path, queryParams);

    final response = await _withClient(
      (client) async => client.get(
        uri,
        headers: (await headers)..addAll(extraHeaders),
      ),
    );
    return _decode(response);
  }

  /// Sends a low-level post request.
  Future<dynamic> post(
    String path,
    Map<String, dynamic> data, {
    Map<String, dynamic>? queryParams,
    Map<String, String> extraHeaders = const {},
  }) async {
    final uri = Uri.https(_authority, _basePath + path, queryParams);

    final response = await _withClient(
      (client) async => client.post(
        uri,
        headers: (await headers)..addAll(extraHeaders),
        body: convert.jsonEncode(data),
      ),
    );
    return _decode(response);
  }

  void _handleError(http.Response response) {
    if (response.statusCode == 500) {
      throw const APIInternalServerException();
    }

    final utf8Body = convert.utf8.decode(response.bodyBytes);
    try {
      final jsonResponse = convert.jsonDecode(utf8Body);
      if (response.statusCode == 401) {
        throw APIAuthenticationException(jsonResponse['message'] ?? utf8Body);
      }
      throw APIGenericException(jsonResponse['message'] ?? utf8Body);
    } on FormatException {
      throw APIGenericException(utf8Body);
    }
  }

  dynamic _decode(http.Response response) {
    if (response.statusCode >= 200 && response.statusCode < 400) {
      final utf8Body = convert.utf8.decode(response.bodyBytes);
      if (utf8Body.isEmpty) {
        return null;
      }
      try {
        return convert.jsonDecode(utf8Body);
      } on FormatException {
        throw APIGenericException(utf8Body);
      }
    }
    _handleError(response);
  }
}

class TestAPIRepository extends APIRepository {
  final http.Client client;

  TestAPIRepository(
    this.client, {
    required super.basePath,
    super.defaultHeaders,
  });

  @override
  http.Client _createClient() => client;
}
