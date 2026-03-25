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

import 'package:flutter/foundation.dart';

/// Tracks the unread status of arbitrary data.
class UnreadController<K> extends ChangeNotifier {
  final _values = <K, dynamic>{};
  final _unreadKeys = <K>{};

  /// Marks [key] as unread if [value] differs from the last call.
  void setValue(K key, dynamic value) {
    if (_values.containsKey(key) && _values[key] == value) {
      return;
    }

    _values[key] = value;
    _unreadKeys.add(key);
    notifyListeners();
  }

  bool isUnread(K key) {
    return _unreadKeys.contains(key);
  }

  void markRead(K key) {
    if (!_unreadKeys.contains(key)) {
      return;
    }

    _unreadKeys.remove(key);
    notifyListeners();
  }

  void markAllRead() {
    if (_unreadKeys.isEmpty) {
      return;
    }

    _unreadKeys.clear();
    notifyListeners();
  }
}
