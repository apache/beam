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

RegExp regExp = RegExp(r'\-\-([A-z0-9]*)\s*([A-z0-9]*)\s*');

const keyValueGroupCount = 2;

String getGroupValue(RegExpMatch match, int groupNum) {
  return match.group(groupNum)?.trim() ?? '';
}

/// Parses pipeline options string (--key value) to the key-value map
Map<String, String>? parsePipelineOptions(String pipelineOptions) {
  final Map<String, String> result = {};
  if (pipelineOptions.isEmpty) {
    return result;
  }
  final matches = regExp.allMatches(pipelineOptions);
  if (matches.isEmpty) {
    return null;
  }
  final hasError = matches
      .where((match) =>
          match.groupCount != keyValueGroupCount ||
          getGroupValue(match, 1).isEmpty ||
          getGroupValue(match, 2).isEmpty)
      .isNotEmpty;
  if (hasError) {
    return null;
  }
  for (var match in matches) {
    final key = getGroupValue(match, 1);
    final value = getGroupValue(match, 2);
    result[key] = value;
  }
  return result;
}

/// Converts pipeline options to --key value string
String pipelineOptionsToString(Map<String, String> pipelineOptions) {
  return pipelineOptions.entries.map((e) => '--${e.key} ${e.value}').join(' ');
}
