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

// TODO(nausharipov): remove after demo

Future<Map<String, dynamic>> kGetSdks() async {
  final response = await http.get(
    Uri.parse(
      'https://us-central1-tour-of-beam-2.cloudfunctions.net/getSdkList',
    ),
  );
  final decodedResponse = jsonDecode(utf8.decode(response.bodyBytes));
  return decodedResponse;
}

Future<Map<String, dynamic>> kGetContentTree() async {
  final response = await http.get(
    Uri.parse(
      'https://us-central1-tour-of-beam-2.cloudfunctions.net/getContentTree?sdk=Python',
    ),
  );
  final decodedResponse = jsonDecode(utf8.decode(response.bodyBytes));
  return decodedResponse;
}
