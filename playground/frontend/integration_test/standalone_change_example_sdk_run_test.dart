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
import 'package:playground/services/analytics/events/snippet_selected.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';
import 'common/widget_tester.dart';

const _outputPrefix = 'The processing has been started\n';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  Future<void> changeToJavaAggregationMax(WidgetTester wt) async {
    await wt.tapAndSettle(find.exampleSelector());
    await wt.tapAndSettle(find.exampleItemInDropdown(javaAggregationMax.name));

    if (areExamplesDeployed) {
      final visibleText = await javaAggregationMax.getVisibleText();
      final lastSpanText =
          wt.findOneCodeController().lastTextSpan!.toPlainText();

      expect(
        lastSpanText.isAsIfCutFrom(visibleText),
        true,
        reason: '$lastSpanText is not as if cut from $visibleText',
      );
    }

    expectLastAnalyticsEvent(
      SnippetSelectedAnalyticsEvent(
        sdk: Sdk.java,
        snippet: javaAggregationMax.dbPath,
      ),
    );
  }

  Future<void> runCustomJava(WidgetTester wt) async {
    const text = 'OK';
    const code = '''
public class MyClass {
  public static void main(String[] args) {
    System.out.print("$text");
  }
}
''';

    await wt.enterText(find.snippetCodeField(), code);
    await wt.pumpAndSettle();

    await wt.tapAndSettle(find.runOrCancelButton());

    expectOutputEqualsIfDeployed('$_outputPrefix$text', wt);
  }

  Future<void> switchToPython(WidgetTester wt) async {
    await wt.changeSdk(Sdk.python);

    if (areExamplesDeployed) {
      final visibleText = await pythonWordCountWithMetrics.getVisibleText();
      final lastSpanText = wt
          .findOneCodeController()
          .lastTextSpan!
          .toPlainText();

      expect(
        lastSpanText.isAsIfCutFrom(visibleText),
        true,
        reason: '$lastSpanText is not as if cut from $visibleText',
      );
    }

    expectLastAnalyticsEvent(const SdkSelectedAnalyticsEvent(sdk: Sdk.python));
  }

  Future<void> changeToPythonAggregationMean(WidgetTester wt) async {
    await wt.tapAndSettle(find.exampleSelector());
    await wt
        .tapAndSettle(find.exampleItemInDropdown(pythonAggregationMean.name));

    // Cannot test this because the DB examples differ from GitHub now.
    // TODO(alexeyinkin): Uncomment when DB is up-to-date.
    // expect(
    //   wt.findOneCodeController().lastTextSpan!.toPlainText().isAsIfCutFrom(
    //     await Examples.getVisibleTextByPath(
    //       ExamplePaths.pythonAggregationMean,
    //       python,
    //     ),
    //   ),
    //   true,
    // );
  }

  Future<void> runCustomPython(WidgetTester wt) async {
    const text = 'OK';
    const code = 'print("$text", end="")';

    await wt.enterText(find.snippetCodeField(), code);
    await wt.pumpAndSettle();

    await wt.tapAndSettle(find.runOrCancelButton());

    expectOutputEqualsIfDeployed('$_outputPrefix$text', wt);
  }

  testWidgets('Change example, change SDK, run', (WidgetTester wt) async {
    await init(wt);

    await changeToJavaAggregationMax(wt);
    await wt.runExpectCached(javaAggregationMax);
    await runCustomJava(wt);

    await switchToPython(wt);
    await changeToPythonAggregationMean(wt);
    await wt.runExpectCached(pythonAggregationMean);
    await runCustomPython(wt);
  });
}
