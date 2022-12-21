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

import 'package:app_state/app_state.dart';
import 'package:flutter/material.dart';

import '../widgets/toasts/toast_listener.dart';

/// Wraps [pageStack] in widgets that must be above [Navigator] and can be
/// below [MaterialApp].
class BeamRouterDelegate extends PageStackRouterDelegate {
  BeamRouterDelegate(super.pageStack);

  @override
  Widget build(BuildContext context) {
    // Overlay: to float toasts.
    // ToastListenerWidget: turns notification events into floating toasts.
    return Overlay(
      initialEntries: [
        OverlayEntry(
          builder: (context) => ToastListenerWidget(
            child: super.build(context),
          ),
        ),
      ],
    );
  }
}
