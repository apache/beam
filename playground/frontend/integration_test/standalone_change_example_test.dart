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

import 'common.dart';

const _fiveSec = Duration(seconds: 5);

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Integration.', () {
    testWidgets('Change example', (WidgetTester wt) async {
      await init(wt);

      expect(
        wt.findOneCodeController().lastTextSpan!.toPlainText(),
        await Examples.getJavaVisibleText(ExamplePaths.javaMinimalWordCount),
      );

      await wt.tap(find.exampleSelector());
      await wt.pumpAndSettle();

      await wt.tap(find.exampleItemInDropdown(ExampleNames.aggregationMax));

      // There is no animation on changing examples, so the above did not wait
      // for the example to load.
      // TODO(alexeyinkin): Add animation, https://github.com/apache/beam/issues/24724
      await Future.delayed(_fiveSec);
      await wt.pumpAndSettle();

      expect(
        wt.findOneCodeController().lastTextSpan!.toPlainText(),
        await Examples.getJavaVisibleText(ExamplePaths.javaAggregationMax),
      );
    });
  });
}
