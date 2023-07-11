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

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'miscellaneous_ui/description_test.dart';
import 'miscellaneous_ui/external_url_navigation.dart';
import 'miscellaneous_ui/feedback_test.dart';
import 'miscellaneous_ui/new_example.dart';
import 'miscellaneous_ui/output_placement_test.dart';
import 'miscellaneous_ui/report_issue.dart';
import 'miscellaneous_ui/resize_output_test.dart';
import 'miscellaneous_ui/shortcuts_modal_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets(
    'Check UI, not connected with running examples',
    (WidgetTester wt) async {
      await init(wt);

      await checkFeedback(wt);
      await checkDescription(wt);
      await checkOutputPlacement(wt);
      await checkResizeOutput(wt);
      await checkShortcutsModal(wt);
      await checkToggleBrightnessMode(wt);
      await checkReportIssue(wt);
      await checkNewExample(wt);
      await checkExternalUrlNavigation(wt);
    },
  );
}
