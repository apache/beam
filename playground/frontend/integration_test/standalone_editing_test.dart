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
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/examples.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Testing editing code', (WidgetTester wt) async {
    await init(wt);
    await _checkResetUnmodifiedCode(wt);
    await _checkResetModifiedCode(wt);
    await _checkAutocomplete(wt);
    await _checkCodeHighlightedMultipleColors(wt);
    await _checkCodeBlockFolding(wt);
  });
}

Future<void> _checkResetUnmodifiedCode(WidgetTester wt) async {
  final playgroundController = wt.findPlaygroundController();

  final code = playgroundController.source;

  expect(code, isNotNull);

  await wt.tapAndSettle(find.resetButton());

  expect(playgroundController.source, code);
  expectLastAnalyticsEvent(
    SnippetResetAnalyticsEvent(snippetContext: defaultEventSnippetContext),
  );
}

Future<void> _checkResetModifiedCode(WidgetTester wt) async {
  final playgroundController = wt.findPlaygroundController();
  final code = playgroundController.source;

  await wt.enterCodeFieldText('print("Hello World!');
  await wt.pumpAndSettle();

  expect(playgroundController.source, isNot(code));
  expectLastAnalyticsEvent(
    SnippetModifiedAnalyticsEvent(
      additionalParams: {},
      fileName: javaMinimalWordCount.mainFileName,
      sdk: defaultEventSnippetContext.sdk!,
      snippet: defaultEventSnippetContext.originalSnippet!,
    ),
  );

  await wt.tapAndSettle(find.resetButton());

  expect(playgroundController.source, code);
  expectLastAnalyticsEvent(
    SnippetResetAnalyticsEvent(
      snippetContext: modifiedDefaultEventSnippetContext,
    ),
  );
}

Future<void> _checkAutocomplete(WidgetTester wt) async {
  // Several newlines are required here because suggestion
  // popup works incorrectly. Remove when fixed
  await wt.enterCodeFieldText('\n\n\n\n\nsdk');

  final playgroundController = wt.findPlaygroundController();
  await wt.runShortcut(playgroundController.showSuggestionsShortcut);
  await wt.pumpAndSettle();

  expect(find.text('sdkHttpMetadata'), findsOneWidget);
  expect(find.text('sdkHttpMetadataWithoutHeaders'), findsOneWidget);
  expect(find.text('sdkHttpResponse'), findsOneWidget);
  expect(find.text('sdkHttpResponseWithoutHeaders'), findsOneWidget);

  await wt.tapAndSettle(find.resetButton());
}

Future<void> _checkCodeHighlightedMultipleColors(WidgetTester wt) async {
  final codeController = wt.findOneCodeController();
  final colors = <Color>{};
  var textSpan = codeController.lastTextSpan;
  _collectTextSpanTreeTextColors(textSpan, colors);

  expect(colors.length, greaterThan(1));
}

void _collectTextSpanTreeTextColors(InlineSpan? span, Set<Color> colors) {
  if (span is TextSpan) {
    if (span.style?.color != null) {
      colors.add(span.style!.color!);
    }
    if (span.children != null) {
      for (final child in span.children!) {
        _collectTextSpanTreeTextColors(child, colors);
      }
    }
  }
}

Future<void> _checkCodeBlockFolding(WidgetTester wt) async {
  const code = '''
public class MyClass {
  public static void main(String[] args) {
    System.out.print("Hello World!");
  }
}
''';

  await wt.enterCodeFieldText(code);
  await wt.pumpAndSettle();

  await wt.tapAndSettle(_getTopToggle(wt));

  const foldedCode = '''
public class MyClass {
''';

  expect(wt.findOneCodeController().text, foldedCode);

  await wt.tapAndSettle(_getTopToggle(wt));

  expect(wt.findOneCodeController().text, code);
}

Finder _getTopToggle(WidgetTester wt) {
  return find
      .descendant(
        of: find.byType(RotatedBox),
        matching: find.byIcon(Icons.chevron_right),
      )
      .verticallyAt(0, wt);
}
