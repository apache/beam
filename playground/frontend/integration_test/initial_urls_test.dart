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

import 'dart:convert';

import 'package:flutter/widgets.dart';
import 'package:flutter_code_editor/flutter_code_editor.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground/services/analytics/events/loaded.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';

// This test loads the app with most of the startup URL combinations.
// They cannot be tested directly due to a Flutter bug:
//     https://github.com/flutter/flutter/issues/118028
// So the test performs in-app navigation to these URLs.
// Many state objects are re-created this way, but not all,
// so the test is not clean.

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  testWidgets('Initial URLs', (WidgetTester wt) async {
    await init(wt);

    await _testEmbeddedRoot(wt);
    await _testStandaloneRoot(wt);

    await _testEmbeddedSdkOnly(wt);
    await _testStandaloneSdkOnly(wt);

    await _testCatalogDefaultExampleLoader(wt);
    await _testContentExampleLoader(wt);
    await _testEmptyExampleLoader(wt);
    await _testHttpExampleLoaderIfDeployed(wt);
    await _testStandardExampleLoader(wt);
    await _testUserSharedExampleLoader(wt);

    await _testMultipleExamples(wt);
  });
}

const _embeddedPath = '/embedded';
const _standalonePath = '/';
const _paths = [_embeddedPath, _standalonePath];

const _fullViewOptionsMap = {
  'readonly': 'readonly1,readonly2',
  'unfold': 'unfold1,unfold2',
};
final _croppedViewOptionsMap = {
  ..._fullViewOptionsMap,
  'show': 'show',
};

final _fullViewOptions = _mapToQueryString(_fullViewOptionsMap);
final _croppedViewOptions = _mapToQueryString(_croppedViewOptionsMap);

Future<void> _testEmbeddedRoot(WidgetTester wt) async {
  await wt.navigateAndSettle(_embeddedPath);
  expectSdk(Sdk.java, wt);
  expectVisibleTextIfDeployed('', wt);
  expectLastAnalyticsEvent(
    const LoadedAnalyticsEvent(
      sdk: Sdk.java,
      snippet: null,
    ),
  );
}

Future<void> _testStandaloneRoot(WidgetTester wt) async {
  final visibleText = await javaMinimalWordCount.getVisibleText();
  await wt.navigateAndSettle(_standalonePath);

  expectSdk(Sdk.java, wt);
  expectVisibleTextIfDeployed(visibleText, wt);
  expectLastAnalyticsEvent(
    const LoadedAnalyticsEvent(
      sdk: Sdk.java,
      snippet: null, // Cannot tell from the initial data.
    ),
  );
}

Future<void> _testEmbeddedSdkOnly(WidgetTester wt) async {
  await wt.navigateAndSettle('$_embeddedPath?sdk=go');
  expectSdk(Sdk.go, wt);
  expectVisibleTextIfDeployed('', wt);
  expectLastAnalyticsEvent(
    const LoadedAnalyticsEvent(
      sdk: Sdk.go,
      snippet: null,
    ),
  );
}

Future<void> _testStandaloneSdkOnly(WidgetTester wt) async {
  final visibleText = await goMinimalWordCount.getVisibleText();
  await wt.navigateAndSettle('$_standalonePath?sdk=go');

  expectSdk(Sdk.go, wt);
  expectVisibleTextIfDeployed(visibleText, wt);
  expectLastAnalyticsEvent(
    const LoadedAnalyticsEvent(
      sdk: Sdk.go,
      snippet: null, // Cannot tell from the initial data.
    ),
  );
}

Future<void> _testCatalogDefaultExampleLoader(WidgetTester wt) async {
  final visibleText = await goMinimalWordCount.getVisibleText();
  await wt.navigateAndSettle('$_standalonePath?sdk=go&default=true');

  expectSdk(Sdk.go, wt);
  expectVisibleTextIfDeployed(visibleText, wt);
  expectLastAnalyticsEvent(
    const LoadedAnalyticsEvent(
      sdk: Sdk.go,
      snippet: null, // Cannot tell from the initial data.
    ),
  );
}

Future<void> _testContentExampleLoader(WidgetTester wt) async {
  final content = await goExample.getFullText();

  for (final path in _paths) {
    final files = jsonEncode([
      {'content': content, 'isMain': true}
    ]);
    await wt.navigateAndSettle(
      '$path?sdk=go&files=${Uri.encodeComponent(files)}&$_fullViewOptions',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed(goExample.foldedVisibleText, wt);
    expectLastAnalyticsEvent(
      const LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: null,
      ),
    );
    await _expectEditableAndReadOnly(wt);

    await wt.navigateAndSettle(
      '$path?sdk=go&files=${Uri.encodeComponent(files)}&$_croppedViewOptions',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed(goExample.croppedFoldedVisibleText, wt);
    _expectReadOnly(wt);
  }
}

Future<void> _testEmptyExampleLoader(WidgetTester wt) async {
  for (final path in _paths) {
    await wt.navigateAndSettle('$path?sdk=go&empty=true');
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed('', wt);
    expectLastAnalyticsEvent(
      const LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: null,
      ),
    );
  }
}

Future<void> _testHttpExampleLoaderIfDeployed(WidgetTester wt) async {
  if (!areExamplesDeployed) {
    return;
  }

  for (final path in _paths) {
    await wt.navigateAndSettle(
      '$path?sdk=go&url=${goExample.rawUrl}&$_fullViewOptions',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed(goExample.foldedVisibleText, wt);
    expectLastAnalyticsEvent(
      LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: goExample.rawUrl,
      ),
    );
    await _expectEditableAndReadOnly(wt);

    await wt.navigateAndSettle(
      '$path?sdk=go&url=${goExample.rawUrl}&$_croppedViewOptions',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed(goExample.croppedFoldedVisibleText, wt);
    expectLastAnalyticsEvent(
      LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: goExample.rawUrl,
      ),
    );
    _expectReadOnly(wt);
  }
}

Future<void> _testStandardExampleLoader(WidgetTester wt) async {
  final visibleText = await goWordCount.getVisibleText();

  for (final path in _paths) {
    await wt.navigateAndSettle(
      '$path?sdk=go&path=${goWordCount.dbPath}',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed(visibleText, wt);
    expectLastAnalyticsEvent(
      LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: goWordCount.dbPath,
      ),
    );
  }
}

Future<void> _testUserSharedExampleLoader(WidgetTester wt) async {
  final template = await goExample.getFullText();
  final tail = '\n//${DateTime.now().millisecondsSinceEpoch}';
  final content = '$template$tail';
  final snippetId = await _getSnippetId(wt, content, Sdk.go);

  for (final path in _paths) {
    await wt.navigateAndSettle(
      '$path?sdk=go&shared=$snippetId&$_fullViewOptions',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed('${goExample.foldedVisibleText}$tail', wt);
    expectLastAnalyticsEvent(
      LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: snippetId,
      ),
    );
    await _expectEditableAndReadOnly(wt);

    await wt.navigateAndSettle(
      '$path?sdk=go&shared=$snippetId&$_croppedViewOptions',
    );
    expectSdk(Sdk.go, wt);
    expectVisibleTextIfDeployed(goExample.croppedFoldedVisibleText, wt);
    expectLastAnalyticsEvent(
      LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: snippetId,
      ),
    );
    _expectReadOnly(wt);
  }
}

Future<String> _getSnippetId(WidgetTester wt, String content, Sdk sdk) async {
  final exampleCache = wt.findPlaygroundController().exampleCache;
  final snippetId = await exampleCache.saveSnippet(
    files: [SnippetFile(content: content, isMain: false, name: 'name')],
    sdk: sdk,
    pipelineOptions: '--name=value',
  );

  print('Created user-shared example ID: $snippetId');

  return snippetId;
}

Future<void> _testMultipleExamples(WidgetTester wt) async {
  final javaTemplate = await javaExample.getFullText();
  final goTemplate= await goExample.getFullText();
  final tail = '\n//${DateTime.now().millisecondsSinceEpoch}';
  final javaSnippetId = await _getSnippetId(wt, '$javaTemplate$tail', Sdk.java);
  final goSnippetId = await _getSnippetId(wt, '$goTemplate$tail', Sdk.go);

  final javaVisibleText = '${javaExample.foldedVisibleText}$tail';
  final goVisibleText = '${goExample.foldedVisibleText}$tail';

  final examplesList = [
    {
      'sdk': Sdk.java.id,
      'shared': javaSnippetId,
      ..._fullViewOptionsMap,
    },
    {
      'sdk': Sdk.go.id,
      'shared': goSnippetId,
      ..._fullViewOptionsMap,
    },
  ];
  final examples = jsonEncode(examplesList);

  for (final path in _paths) {
    await wt.navigateAndSettle('$path?sdk=go&examples=$examples');
    expectSdk(Sdk.go, wt);
    expectVisibleText(goVisibleText, wt, reason: 'go, $path');
    expectLastAnalyticsEvent(
      LoadedAnalyticsEvent(
        sdk: Sdk.go,
        snippet: goSnippetId,
      ),
    );
    await _expectEditableAndReadOnly(wt);

    final playgroundController = wt.findPlaygroundController();
    playgroundController.setSdk(Sdk.java);
    await wt.pumpAndSettle();

    expectSdk(Sdk.java, wt);
    expectVisibleText(javaVisibleText, wt, reason: 'java, $path');
    await _expectEditableAndReadOnly(wt);
  }
}

/// Checks that the example contains:
/// - at least two 'editable' substrings, and they are editable,
/// - at least two 'readonly' substrings, and they are read-only.
Future<void> _expectEditableAndReadOnly(WidgetTester wt) async {
  final controller = wt.findOneCodeController();

  for (int i = 0; i < 2; i++) {
    controller.value = controller.value.select('editable')!;

    final edited = controller.value.typed('');
    controller.value = edited;
    await wt.pumpAndSettle();
    expect(controller.value, edited, reason: 'Cannot edit an editable line');

    controller.value = controller.value.select('readonly')!.typed('');
    await wt.pumpAndSettle();
    expect(controller.value, edited, reason: 'Can edit a read-only line');
  }
}

/// Checks that every character in the editor is read-only.
void _expectReadOnly(WidgetTester wt) {
  final controller = wt.findOneCodeController();
  final value = controller.value;
  final text = controller.text;

  for (int i = text.length; --i >= 0;) {
    controller.value = value
        .copyWith(selection: TextSelection.collapsed(offset: i))
        .typed('a');
    expect(controller.value, value, reason: 'Can edit a read-only controller');
  }
}

String _mapToQueryString(Map<String, String> map) {
  return [for (final e in map.entries) '${e.key}=${e.value}'].join('&');
}
