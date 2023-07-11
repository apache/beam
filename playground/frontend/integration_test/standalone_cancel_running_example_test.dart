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

const kExecutionCancelledText = 'Pipeline cancelled';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Cancel running example', (WidgetTester wt) async {
    await init(wt);

    // Cancel unchanged example.
    await _runAndCancelExample(wt, const Duration(milliseconds: 300));

    final source = wt.findPlaygroundController().source ?? '';
    await wt.enterText(find.snippetCodeField(), '//comment\n' + source);
    await wt.pumpAndSettle();

    // Cancel changed example.
    await _runAndCancelExample(wt, const Duration(milliseconds: 5000));
  });
}

Future<void> _runAndCancelExample(WidgetTester wt, Duration duration) async {
  await wt.tap(find.runOrCancelButton());

  final playgroundController = wt.findPlaygroundController();
  final eventSnippetContext = playgroundController.eventSnippetContext;
  expectLastAnalyticsEvent(
    RunStartedAnalyticsEvent(
      snippetContext: eventSnippetContext,
      trigger: EventTrigger.click,
    ),
  );

  await wt.pumpAndSettleNoException(timeout: duration);
  await wt.tapAndSettle(find.runOrCancelButton());

  expect(
    playgroundController.codeRunner.resultLogOutput,
    contains(kExecutionCancelledText),
  );

  final event = PlaygroundComponents.analyticsService.lastEvent;
  expect(event, isA<RunCancelledAnalyticsEvent>());

  final cancelEvent = event! as RunCancelledAnalyticsEvent;
  expect(cancelEvent.snippetContext, eventSnippetContext);
  expect(cancelEvent.trigger, EventTrigger.click);
}
