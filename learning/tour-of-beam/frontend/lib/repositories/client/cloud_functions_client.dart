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

import 'package:http/http.dart' as http;

import '../../config.dart';
import '../../models/content_tree.dart';
import '../models/get_sdks_response.dart';
import 'client.dart';

class CloudFunctionsTobClient extends TobClient {
  @override
  Future<GetSdksResponse> getSdks() async {
    final json = await http.get(
      Uri.parse(
        '$cloudFunctionsBaseUrl/getSdkList',
      ),
    );

    final map = jsonDecode(utf8.decode(json.bodyBytes)) as Map<String, dynamic>;
    return GetSdksResponse.fromJson(map);
  }

  @override
  Future<ContentTreeModel> getContentTree() async {
    final json = await http.get(
      Uri.parse(
        '$cloudFunctionsBaseUrl/getContentTree?sdk=Python',
      ),
    );
    final map = jsonDecode(utf8.decode(json.bodyBytes)) as Map<String, dynamic>;
    return ContentTreeModel.fromJson(map);
  }
}
