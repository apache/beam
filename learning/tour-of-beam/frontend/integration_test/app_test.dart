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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:tour_of_beam/main.dart' as app;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('theme', () {
    testWidgets('mode toggle', (tester) async {
      app.main();
      await tester.pumpAndSettle();
      final Finder switchToDarkModeButton =
          find.widgetWithText(ToggleThemeButton, 'ui.darkMode'.tr());
      expect(switchToDarkModeButton, findsOneWidget);
      await tester.tap(switchToDarkModeButton);
      await tester.pumpAndSettle();
      expect(
        find.widgetWithText(ToggleThemeButton, 'ui.lightMode'.tr()),
        findsOneWidget,
      );
    });
  });
}
