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

import 'dart:async';

import 'package:easy_localization/easy_localization.dart';
import 'package:rxdart/rxdart.dart';

import '../exceptions/detailed_exception.dart';
import '../models/toast.dart';
import '../models/toast_type.dart';

/// Communicates popup notifications.
///
/// Objects that need to publish an app-wide notifications
/// should submit them with [add] or [addException].
class ToastNotifier {
  final _controller = BehaviorSubject<Toast>();

  /// The stream of notification objects to be shown as popups.
  Stream<Toast> get toasts => _controller.stream;

  /// Adds a [toast] to the [toasts] stream to be shown as a popup.
  void add(Toast toast) {
    _controller.add(toast);
  }

  /// Adds an [exception] to be shown as an error popup.
  void addException(Exception exception) {
    add(
      Toast(
        title: 'errors.error'.tr(),
        description: exception.toString(),
        type: ToastType.error,
      ),
    );

    _logExceptionToConsole(exception);
  }

  void _logExceptionToConsole(Exception exception) {
    final buffer = StringBuffer('$exception\n');

    if (exception is DetailedException) {
      buffer.writeln(exception.details);
    }

    print(buffer); // ignore: avoid_print
  }

  Future<void> dispose() async {
    await _controller.close();
  }
}
