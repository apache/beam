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

import 'dart:convert';
import 'dart:io';

import 'package:get_it/get_it.dart';
import 'package:http/http.dart' as http;
import 'package:playground_components/playground_components.dart';

import '../../auth/notifier.dart';
import '../../config.dart';
import '../../models/content_tree.dart';
import '../../models/unit_content.dart';
import '../models/get_content_tree_response.dart';
import '../models/get_sdks_response.dart';
import '../models/get_user_progress_response.dart';
import 'client.dart';

enum RequestMethod {
  post,
  get,
}

class CloudFunctionsTobClient extends TobClient {
  Future<dynamic> _makeRequest({
    required String path,
    required RequestMethod method,
    Map<String, dynamic> queryParameters = const {},
    dynamic body,
  }) async {
    final token = await GetIt.instance.get<AuthNotifier>().getToken();
    final uri = Uri.parse('$cloudFunctionsBaseUrl$path')
        .replace(queryParameters: queryParameters);
    final headers = token != null
        ? {HttpHeaders.authorizationHeader: 'Bearer $token'}
        : null;

    http.Response response;
    switch (method) {
      case RequestMethod.post:
        response = await http.post(
          uri,
          headers: headers,
          body: body,
        );
        break;
      case RequestMethod.get:
        response = await http.get(
          uri,
          headers: headers,
        );
        break;
    }
    return jsonDecode(utf8.decode(response.bodyBytes));
  }

  @override
  Future<GetSdksResponse> getSdks() async {
    final map = await _makeRequest(
      method: RequestMethod.get,
      path: 'getSdkList',
    );
    return GetSdksResponse.fromJson(map);
  }

  @override
  Future<ContentTreeModel> getContentTree(String sdkId) async {
    final map = await _makeRequest(
      method: RequestMethod.get,
      path: 'getContentTree',
      queryParameters: {
        'sdk': sdkId,
      },
    );
    final response = GetContentTreeResponse.fromJson(map);
    return ContentTreeModel.fromResponse(response);
  }

  @override
  Future<UnitContentModel> getUnitContent(String sdkId, String unitId) async {
    final map = await _makeRequest(
      method: RequestMethod.get,
      path: 'getUnitContent',
      queryParameters: {
        'sdk': sdkId,
        'id': unitId,
      },
    );
    return UnitContentModel.fromJson(map);
  }

  @override
  Future<GetUserProgressResponse?> getUserProgress(String sdkId) async {
    final token = await GetIt.instance.get<AuthNotifier>().getToken();
    if (token == null) {
      return null;
    }
    final map = await _makeRequest(
      method: RequestMethod.get,
      path: 'getUserProgress',
      queryParameters: {
        'sdk': sdkId,
      },
    );
    final response = GetUserProgressResponse.fromJson(map);
    return response;
  }

  @override
  Future<void> postUnitComplete(String sdkId, String id) async {
    await _makeRequest(
      method: RequestMethod.post,
      path: 'postUnitComplete',
      queryParameters: {
        'sdk': sdkId,
        'id': id,
      },
    );
  }

  @override
  Future<void> postDeleteUserProgress() async {
    await _makeRequest(
      method: RequestMethod.post,
      path: 'postDeleteProgress',
    );
  }

  @override
  Future<void> postUserCode({
    required List<SnippetFile> snippetFiles,
    required String sdkId,
    required String unitId,
  }) async {
    await _makeRequest(
      path: 'postUserCode',
      method: RequestMethod.post,
      queryParameters: {
        'sdk': sdkId,
        'id': unitId,
      },
      body: jsonEncode({
        'files': snippetFiles.map((file) => file.toJson()).toList(),
        'pipelineOptions': '',
      }),
    );
  }
}
