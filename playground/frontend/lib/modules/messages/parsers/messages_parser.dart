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

import 'package:playground/modules/messages/models/abstract_message.dart';
import 'package:playground/modules/messages/models/set_content_message.dart';
import 'package:playground/modules/messages/models/set_sdk_message.dart';

class MessagesParser {
  AbstractMessage? tryParse(Object? data) {
    return _tryParseIfMap(data) ?? _tryParseIfJson(data);
  }

  AbstractMessage? _tryParseIfMap(Object? map) {
    return map is Map ? _tryParseMap(map) : null;
  }

  AbstractMessage? _tryParseMap(Map map) {
    return SetContentMessage.tryParse(map) ?? SetSdkMessage.tryParse(map);
  }

  AbstractMessage? _tryParseIfJson(Object? json) {
    if (json is String) {
      try {
        final map = jsonDecode(json);

        if (map is Map) {
          return _tryParseMap(map);
        }
      } on FormatException catch (ex) {
        // TODO: Log
        print('_tryParseIfJson FormatException: $ex');
      }
    }

    return null;
  }
}
