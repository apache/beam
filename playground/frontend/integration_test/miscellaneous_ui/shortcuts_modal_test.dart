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
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/pages/standalone_playground/widgets/more_actions.dart';
import 'package:playground/services/analytics/events/shortcuts_clicked.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import '../common/common_finders.dart';

Future<void> checkShortcutsModal(WidgetTester wt) async {
  expect(find.shortcutsModal(), findsNothing);

  await wt.tapAndSettle(find.moreActions());

  final menuItem = find.menuItem(HeaderAction.shortcuts);
  expect(menuItem, findsOneWidget);

  await wt.tapAndSettle(menuItem);

  expect(find.shortcutsModal(), findsOneWidget);
  expectLastAnalyticsEvent(const ShortcutsClickedAnalyticsEvent());

  await wt.sendKeyEvent(LogicalKeyboardKey.escape);
  await wt.pumpAndSettle();

  expect(find.shortcutsModal(), findsNothing);
}
