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
import 'package:playground_components/playground_components.dart';

import '../common/common_finders.dart';

Future<void> checkEnjoyPlayground(WidgetTester wt) async {
  await _checkEnjoyingAndSendFeedback(wt);
  await _checkNotEnjoyingAndSendFeedback(wt);
  await _checkNotEnjoyingAndClose(wt);
}

Future<void> _checkNotEnjoyingAndClose(WidgetTester wt) async {
  await wt.tap(find.feedbackThumbDown());
  await wt.pumpAndSettle();

  expect(find.feedbackDropdownContent(), findsOneWidget);

  await wt.tap(find.feedbackDropdownCancelButton());
  await wt.pumpAndSettle();

  expect(find.feedbackDropdownContent(), findsNothing);
}

Future<void> _checkEnjoyingAndSendFeedback(WidgetTester wt) async {
  expect(find.feedbackDropdownContent(), findsNothing);

  await wt.tap(find.feedbackThumbUp());
  await wt.pumpAndSettle();

  expect(find.feedbackDropdownContent(), findsOneWidget);

  const text = 'This is enjoying text';
  await wt.enterText(find.feedbackDropdownTextField(), text);
  await wt.pumpAndSettle();

  expect(find.text(text), findsOneWidget);

  await wt.tap(find.feedbackDropdownSendButton());
  await wt.pumpAndSettle();

  final lastEvent = PlaygroundComponents.analyticsService.lastEvent;
  expect(
    lastEvent,
    const FeedbackFormSentAnalyticsEvent(
      snippetContext: EventSnippetContext(
        // TODO: Replace with values from the example object when merged https://github.com/apache/beam/pull/25034
        originalSnippet: 'SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MinimalWordCount',
        sdk: Sdk.java,
        snippet: 'SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MinimalWordCount',
      ),
      rating: FeedbackRating.positive,
      text: text,
    ),
  );

  expect(find.feedbackDropdownContent(), findsNothing);
}

Future<void> _checkNotEnjoyingAndSendFeedback(WidgetTester wt) async {
  await wt.tap(find.feedbackThumbDown());
  await wt.pumpAndSettle();

  expect(find.feedbackDropdownContent(), findsOneWidget);

  const text = 'This is not enjoying text';
  await wt.enterText(find.feedbackDropdownTextField(), text);
  await wt.pumpAndSettle();

  expect(find.text(text), findsOneWidget);

  await wt.tap(find.feedbackDropdownSendButton());
  await wt.pumpAndSettle();

  final lastEvent = PlaygroundComponents.analyticsService.lastEvent;
  expect(
    lastEvent,
    const FeedbackFormSentAnalyticsEvent(
      snippetContext: EventSnippetContext(
        // TODO: Replace with values from the example object when merged https://github.com/apache/beam/pull/25034
        originalSnippet: 'SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MinimalWordCount',
        sdk: Sdk.java,
        snippet: 'SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MinimalWordCount',
      ),
      rating: FeedbackRating.negative,
      text: text,
    ),
  );

  expect(find.feedbackDropdownContent(), findsNothing);
}
