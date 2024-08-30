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
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:get_it/get_it.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components_dev/playground_components_dev.dart';
import 'package:tour_of_beam/cache/content_tree.dart';
import 'package:tour_of_beam/cache/sdk.dart';
import 'package:tour_of_beam/main.dart' as app;
import 'package:tour_of_beam/state.dart';

import 'common/common_finders.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets('mode toggle', (wt) async {
    await app.main();
    await wt.pumpAndSettle();
    await checkToggleBrightnessMode(wt);
    await _checkSdksLoadedCorrectly(wt);
    await _checkSwitchingSdkWorksCorrectly(wt);
  });
}

Future<void> _checkSdksLoadedCorrectly(WidgetTester wt) async {
  // The source of truth of loaded sdks.
  final sdkCache = GetIt.instance.get<SdkCache>();

  final sdks = sdkCache.getSdks();
  for (final sdk in sdks) {
    expect(
      find.outlinedButtonWithText(sdk.title),
      findsOneWidget,
    );
  }

  final button = wt.widget<ElevatedButton>(find.startTourButton());
  expect(button.onPressed, isNotNull); // Verify it is enabled.

  await wt.tapAndSettle(find.sdkDropdown());

  for (final sdk in sdks) {
    expect(
      find.dropdownMenuItemWithText(sdk.title),
      // The current Flutter implementation builds 2 for some reason.
      findsAtLeastNWidgets(1),
    );
  }

  await wt.sendKeyEvent(LogicalKeyboardKey.escape);
  await wt.pumpAndSettle();
}

Future<void> _checkSwitchingSdkWorksCorrectly(WidgetTester wt) async {
  _checkModulesDisplayed();

  final sdkCache = GetIt.instance.get<SdkCache>();

  final secondSdk = sdkCache.getSdks()[1];
  await wt.tapAndSettle(find.outlinedButtonWithText(secondSdk.title));

  _checkModulesDisplayed();

  await wt.tapAndSettle(find.sdkDropdown());
  final thirdSdk = sdkCache.getSdks()[2];

  await wt.tapAndSettle(
    find.dropdownMenuItemWithText(thirdSdk.title).first,
  );

  _checkModulesDisplayed();
}

void _checkModulesDisplayed() {
  final contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final appNotifier = GetIt.instance.get<AppNotifier>();
  final sdkId = appNotifier.sdk;

  final contentTree = contentTreeCache.getContentTree(sdkId);
  if (contentTree == null) {
    throw Exception('contentTree is null');
  }

  for (final module in contentTree.nodes) {
    expect(find.text(module.title), findsOneWidget);
  }
}
