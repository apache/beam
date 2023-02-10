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

import '../../../playground_components.dart';

/// Groups common overlays.
class BeamOverlays {
  /// Shows an overlay until future completes.
  static Future<void> showProgressOverlay(
    BuildContext context,
    Future future,
  ) async {
    final closeNotifier = PublicNotifier();
    showOverlay(
      context: context,
      closeNotifier: closeNotifier,
      barrierDismissible: false,
      positioned: Positioned.fill(
        child: Container(
          alignment: Alignment.center,
          color: Theme.of(context).dialogBackgroundColor.withOpacity(0.2),
          child: const CircularProgressIndicator(),
        ),
      ),
    );
    await future;
    closeNotifier.notifyPublic();
  }
}
