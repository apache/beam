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

import 'package:highlight/highlight_core.dart';
import 'package:http/http.dart' as http;

import 'code.dart';

class Examples {
  static const _repoAndBranch = 'apache/beam/master';

  static Future<String> getVisibleTextByPath(String path, Mode language) async {
    final uri =
        Uri.parse('https://raw.githubusercontent.com/$_repoAndBranch$path');
    final response = await http.get(uri);
    final content = response.body;

    return foldLicenseAndImports(content, language);
  }
}
