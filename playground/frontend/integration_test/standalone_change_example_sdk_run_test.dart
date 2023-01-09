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
import 'package:highlight/languages/java.dart';
import 'package:highlight/languages/python.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

const _outputPrefix = 'The processing has started\n';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  /// Runs and expects that the execution is as fast as it should be for cache.
  Future<void> runExpectCached(WidgetTester wt) async {
    final dateTimeStart = DateTime.now();

    await wt.tap(find.runOrCancelButton());
    await wt.pumpAndSettle();

    expect(
      DateTime.now().difference(dateTimeStart),
      lessThan(const Duration(milliseconds: 2000)),
    );
  }

  Future<void> expectJavaMinimalWordCount(WidgetTester wt) async {
    expect(
      wt.findOneCodeController().lastTextSpan!.toPlainText().isAsIfCutFrom(
        await Examples.getVisibleTextByPath(
          ExamplePaths.javaMinimalWordCount,
          java,
        ),
      ),
      true,
    );

    expect(find.graphTab(), findsOneWidget);
    expect(find.resultTab(), findsOneWidget);
    expect(wt.findOutputTabController().index, 0);
  }

  Future<void> changeToJavaAggregationMax(WidgetTester wt) async {
    await wt.tap(find.exampleSelector());
    await wt.pumpAndSettle();

    await wt.tap(find.exampleItemInDropdown(ExampleNames.aggregationMax));
    await wt.pumpAndSettle();

    expect(
      wt.findOneCodeController().lastTextSpan!.toPlainText().isAsIfCutFrom(
        await Examples.getVisibleTextByPath(
          ExamplePaths.javaAggregationMax,
          java,
        ),
      ),
      true,
    );
  }

  Future<void> runExpectJavaAggregationMax(WidgetTester wt) async {
    await runExpectCached(wt);
    expectOutputEndsWith(ExampleOutputs.javaAggregationMaxTail, wt);
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

    await wt.enterText(find.codeField(), code);
    await wt.pumpAndSettle();

    await wt.tap(find.runOrCancelButton());
    await wt.pumpAndSettle();

    expectOutput('$_outputPrefix$text', wt);
  }

  Future<void> switchToPython(WidgetTester wt) async {
    await wt.tap(find.sdkSelector());
    await wt.pumpAndSettle();

    await wt.tap(find.sdkItemInDropdown(Sdk.python));
    await wt.pumpAndSettle();

    expect(
      wt.findOneCodeController().lastTextSpan!.toPlainText().isAsIfCutFrom(
            await Examples.getVisibleTextByPath(
              ExamplePaths.pythonMinimalWordCountWithMetrics,
              python,
            ),
          ),
      true,
    );
  }

  Future<void> changeToPythonAggregationMean(WidgetTester wt) async {
    await wt.tap(find.exampleSelector());
    await wt.pumpAndSettle();

    await wt.tap(find.exampleItemInDropdown(ExampleNames.aggregationMean));
    await wt.pumpAndSettle();

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

  Future<void> runExpectPythonAggregationMean(WidgetTester wt) async {
    await runExpectCached(wt);
    expectOutputContains(ExampleOutputs.pythonAggregationMeanContains, wt);
  }

  Future<void> runCustomPython(WidgetTester wt) async {
    const text = 'OK';
    const code = 'print("$text", end="")';

    await wt.enterText(find.codeField(), code);
    await wt.pumpAndSettle();

    await wt.tap(find.runOrCancelButton());
    await wt.pumpAndSettle();

    expectOutput('$_outputPrefix$text', wt);
  }

  testWidgets('Change example, change SDK, run', (WidgetTester wt) async {
    await init(wt);

    await expectJavaMinimalWordCount(wt);
    await changeToJavaAggregationMax(wt);
    await runExpectJavaAggregationMax(wt);
    await runCustomJava(wt);

    await switchToPython(wt);
    await changeToPythonAggregationMean(wt);
    await runExpectPythonAggregationMean(wt);
    await runCustomPython(wt);
  });
}
