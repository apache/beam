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

import 'package:flutter/widgets.dart';
import 'package:visibility_detector/visibility_detector.dart';

import '../../controllers/unread_controller.dart';

/// Clears the unread status of [unreadKey] when built and at least
/// partially visible.
class UnreadClearer<T> extends StatelessWidget {
  const UnreadClearer({
    super.key,
    required this.child,
    required this.controller,
    required this.unreadKey,
  });

  final Widget child;
  final UnreadController<T> controller;
  final T unreadKey;

  @override
  Widget build(BuildContext context) {
    return VisibilityDetector(
      key: Key(unreadKey.toString()),
      onVisibilityChanged: (info) {
        if (info.visibleFraction > 0) {
          controller.markRead(unreadKey);
        }
      },
      child: AnimatedBuilder(
        animation: controller,
        builder: (context, _) {
          WidgetsBinding.instance.addPostFrameCallback((_) {
            controller.markRead(unreadKey);
          });
          return child;
        },
      ),
    );
  }
}
