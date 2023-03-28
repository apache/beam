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

class ProgressDialog extends StatelessWidget {
  const ProgressDialog();

  /// Shows a dialog with [CircularProgressIndicator] until [future] completes.
  static void show({
    required Future future,
    required GlobalKey<NavigatorState> navigatorKey,
  }) {
    var shown = true;
    unawaited(
      showDialog(
        barrierDismissible: false,
        context: navigatorKey.currentContext!,
        builder: (_) => const ProgressDialog(),
      ).whenComplete(() {
        shown = false;
      }),
    );
    unawaited(
      future.whenComplete(() {
        if (shown) {
          navigatorKey.currentState!.pop();
        }
      }),
    );
  }

  @override
  Widget build(BuildContext context) {
    return const Dialog(
      backgroundColor: Colors.transparent,
      child: Center(
        child: CircularProgressIndicator(),
      ),
    );
  }
}
