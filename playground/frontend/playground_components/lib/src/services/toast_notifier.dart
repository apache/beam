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

import '../models/toast.dart';
import '../models/toast_type.dart';

class ToastNotifier {
  final _controller = BehaviorSubject<Toast>();

  Stream<Toast> get toasts => _controller.stream;

  void add(Toast toast) {
    _controller.add(toast);
  }

  void addException(Exception ex) {
    add(
      Toast(
        title: 'errors.error'.tr(),
        description: ex.toString(),
        type: ToastType.error,
      ),
    );
  }

  Future<void> dispose() async {
    await _controller.close();
  }
}
