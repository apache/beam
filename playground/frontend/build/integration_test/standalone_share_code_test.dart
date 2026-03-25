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

import 'package:flutter/cupertino.dart';
import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground/modules/editor/components/share_dropdown/link_text_field.dart';
import 'package:playground/modules/editor/components/share_dropdown/share_tabs/share_format_enum.dart';
import 'package:playground/pages/standalone_playground/path.dart';
import 'package:playground/router/route_information_parser.dart';
import 'package:playground/services/analytics/events/share_code_clicked.dart';
import 'package:playground/services/analytics/events/shareable_copied.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';
import 'common/examples.dart';
import 'common/widget_tester.dart';

const _sdk = Sdk.go;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Share code test', (WidgetTester wt) async {
    await init(wt);

    await wt.changeSdk(_sdk);
    await _shareUnmodifiedCode(wt);
    await _shareModifiedCode(wt);
  });
}

Future<void> _shareUnmodifiedCode(WidgetTester wt) async {
  final eventSnippetContext = wt.findPlaygroundController().eventSnippetContext;

  await wt.tapAndSettle(find.shareButton());

  expectLastAnalyticsEvent(
    ShareCodeClickedAnalyticsEvent(
      snippetContext: eventSnippetContext,
    ),
  );

  final descriptor = await _requireShareableDescriptor(wt);
  await _checkUnmodifiedLink(wt, descriptor, eventSnippetContext);
  await _checkEmbedding(wt, descriptor, eventSnippetContext);

  // Tap outside of the popover to close it.
  // TODO(alexeyinkin): Support 'escape' to close popovers, https://github.com/apache/beam/issues/24371
  await wt.tapAndSettle(find.shareButton(), warnIfMissed: false);
}

Future<void> _checkUnmodifiedLink(
  WidgetTester wt,
  ExampleLoadingDescriptor descriptor,
  EventSnippetContext eventSnippetContext,
) async {
  expect(
    descriptor,
    StandardExampleLoadingDescriptor(
      sdk: _sdk,
      path: defaultExamples[_sdk]!.dbPath,
    ),
  );

  await wt.tapAndSettle(find.copyButton());

  expectLastAnalyticsEvent(
    ShareableCopiedAnalyticsEvent(
      shareFormat: ShareFormat.linkStandalone,
      snippetContext: eventSnippetContext,
    ),
  );
}

Future<void> _checkEmbedding(
  WidgetTester wt,
  ExampleLoadingDescriptor descriptor,
  EventSnippetContext eventSnippetContext,
) async {
  await wt.tapAndSettle(find.shareEmbedTabHeader());

  final text = _requireShareableText(wt);
  expect(text, contains(descriptor.token));

  await wt.tapAndSettle(find.copyButton());

  expectLastAnalyticsEvent(
    ShareableCopiedAnalyticsEvent(
      shareFormat: ShareFormat.iframeEmbedded,
      snippetContext: eventSnippetContext,
    ),
  );
}

Future<void> _shareModifiedCode(WidgetTester wt) async {
  wt.modifyCodeController();
  final eventSnippetContext = wt.findPlaygroundController().eventSnippetContext;

  await wt.tapAndSettle(find.shareButton());

  expectLastAnalyticsEvent(
    ShareCodeClickedAnalyticsEvent(
      snippetContext: eventSnippetContext,
    ),
  );

  final descriptor = await _requireShareableDescriptor(wt);
  await _checkModifiedLink(wt, descriptor, eventSnippetContext);
  await _checkEmbedding(wt, descriptor, eventSnippetContext);

  await wt.sendKeyEvent(LogicalKeyboardKey.escape);
}

Future<void> _checkModifiedLink(
  WidgetTester wt,
  ExampleLoadingDescriptor descriptor,
  EventSnippetContext eventSnippetContext,
) async {
  expect(descriptor, isA<UserSharedExampleLoadingDescriptor>());
  expect(descriptor.sdk, _sdk);

  await wt.tapAndSettle(find.copyButton());

  expectLastAnalyticsEvent(
    ShareableCopiedAnalyticsEvent(
      shareFormat: ShareFormat.linkStandalone,
      snippetContext: eventSnippetContext,
    ),
  );
}

/// Returns the descriptor of a parsed URL in the link text field.
///
/// Can only be called while on the 'Link' tab.
Future<ExampleLoadingDescriptor> _requireShareableDescriptor(
  WidgetTester wt,
) async {
  final path = await _requireShareablePath(wt);
  return path.descriptor;
}

Future<StandalonePlaygroundSingleFirstPath> _requireShareablePath(
  WidgetTester wt,
) async {
  final text = _requireShareableText(wt);

  final path = await PlaygroundRouteInformationParser().parsePagePath(
    RouteInformation(location: text),
  );

  expect(path, isA<StandalonePlaygroundSingleFirstPath>());
  return path as StandalonePlaygroundSingleFirstPath;
}

String _requireShareableText(WidgetTester wt) {
  final textFieldFinder = find.shareableTextField();
  expect(textFieldFinder, findsOneWidget);

  final textField = textFieldFinder.evaluate().first.widget;
  expect(textField, isA<LinkTextField>());
  return (textField as LinkTextField).text;
}
