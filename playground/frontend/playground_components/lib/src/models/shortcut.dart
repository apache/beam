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

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import 'intents.dart';

class BeamShortcut {
  // Keys in the order to be shown or mocked.
  //
  // A list is required because a [LogicalKeySet] discards the original order.
  final List<LogicalKeyboardKey> keys;
  
  LogicalKeySet get keySet => LogicalKeySet.fromSet(keys.toSet());
  
  final BeamIntent actionIntent;
  final CallbackAction Function(BuildContext) createAction;

  BeamShortcut({
    required this.keys,
    required this.actionIntent,
    required this.createAction,
  });

  static const _metaKeyName = 'Command';
  static const _glue = ' + ';

  String get title {
    return keys
        .map(_getKeyDisplayName)
        .join(_glue);
  }

  String _getKeyDisplayName(LogicalKeyboardKey e) {
    if (e.keyId == LogicalKeyboardKey.meta.keyId) {
      return _metaKeyName;
    }
    return e.keyLabel;
  }
}
