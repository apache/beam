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

import 'package:flutter/services.dart';
import 'package:playground/modules/shortcuts/models/shortcut.dart';

const kMetaKeyName = 'CMD/CTRL';
const kShortcutKeyJoinSymbol = ' + ';

String getShortcutDisplayName(Shortcut shortcut) {
  return shortcut.shortcuts.keys
      .map((e) => getKeyDisplayName(e))
      .join(kShortcutKeyJoinSymbol);
}

String getKeyDisplayName(LogicalKeyboardKey e) {
  if (e.keyId == LogicalKeyboardKey.meta.keyId) {
    return kMetaKeyName;
  }
  return e.keyLabel;
}
