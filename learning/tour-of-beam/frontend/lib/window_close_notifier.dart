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

// ignore: avoid_web_libraries_in_flutter
import 'dart:html';

import 'package:flutter/material.dart';

// TODO(nausharipov): review
// Moved the file from PGC, because it causes test failure:
// Error: The getter 'window' isn't defined for the class 'WindowCloseNotifier'.
//  - 'WindowCloseNotifier' is from 'package:playground_components/src/controllers/window_close_notifier.dart' ('lib/src/controllers/window_close_notifier.dart').
// Try correcting the name to the name of an existing getter, or defining a getter or field named 'window'.
//     window.onBeforeUnload.listen((_) {
//     ^^^^^^

class WindowCloseNotifier extends ChangeNotifier {
  WindowCloseNotifier() {
    window.onBeforeUnload.listen((_) {
      notifyListeners();
    });
  }
}
