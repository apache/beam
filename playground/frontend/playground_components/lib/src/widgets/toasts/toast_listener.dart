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

import 'package:flutter/material.dart';
import 'package:fluttertoast/fluttertoast.dart' show FToast, ToastGravity;
import 'package:get_it/get_it.dart';

import '../../constants/durations.dart';
import '../../models/toast.dart';
import '../../services/toast_notifier.dart';
import 'toast.dart';

/// Turns events from [ToastNotifier] into floating [ToastWidget]s.
class ToastListenerWidget extends StatefulWidget {
  final Widget child;

  const ToastListenerWidget({
    super.key,
    required this.child,
  });

  @override
  State<ToastListenerWidget> createState() => _ToastListenerWidgetState();
}

class _ToastListenerWidgetState extends State<ToastListenerWidget> {
  final _notifier = GetIt.instance.get<ToastNotifier>();
  final _flutterToast = FToast();
  StreamSubscription? _toastSubscription;

  @override
  void initState() {
    super.initState();
    _flutterToast.init(context);
    _toastSubscription = _notifier.toasts.listen(_onToast);
  }

  Future<void> _onToast(Toast toast) async {
    _flutterToast.showToast(
      gravity: ToastGravity.TOP,
      toastDuration: BeamDurations.toast,
      child: ToastWidget(toast),
    );
  }

  @override
  Widget build(BuildContext context) {
    return widget.child;
  }

  @override
  void dispose() {
    unawaited(_toastSubscription?.cancel());
    super.dispose();
  }
}
