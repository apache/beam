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
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';
import 'common/widget_tester.dart';

const _outputPrefix = 'The processing has started\n';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  Future<void> changeToJavaAggregationMax(WidgetTester wt) async {
    await wt.tap(find.exampleSelector());
    await wt.pumpAndSettle();

    await wt.tap(find.exampleItemInDropdown(javaAggregationMax.name));
    await wt.pumpAndSettle();

    expect(
      wt.findOneCodeController().lastTextSpan!.toPlainText().isAsIfCutFrom(
            await javaAggregationMax.getVisibleText(),
          ),
      true,
    );
  }

  Future<void> runExpectJavaAggregationMax(WidgetTester wt) async {
    await wt.runExpectCached();
    expectOutputEndsWith(javaAggregationMax.outputTail, wt);
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
    await wt.changeSdk(Sdk.python);

    expect(
      wt.findOneCodeController().lastTextSpan!.toPlainText().isAsIfCutFrom(
            await pythonWordCountWithMetrics.getVisibleText(),
          ),
      true,
    );
  }

  Future<void> changeToPythonAggregationMean(WidgetTester wt) async {
    await wt.tap(find.exampleSelector());
    await wt.pumpAndSettle();

    await wt.tap(find.exampleItemInDropdown(pythonAggregationMean.name));
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
    await wt.runExpectCached();

    for (final str in pythonAggregationMean.outputContains!) {
      expectOutputContains(str, wt);
    }
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

    await changeToJavaAggregationMax(wt);
    await runExpectJavaAggregationMax(wt);
    await runCustomJava(wt);

    await switchToPython(wt);
    await changeToPythonAggregationMean(wt);
    await runExpectPythonAggregationMean(wt);
    await runCustomPython(wt);
  });
}
