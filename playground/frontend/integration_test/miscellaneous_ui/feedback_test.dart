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
import 'package:playground_components_dev/playground_components_dev.dart';

import '../common/common_finders.dart';
import '../common/examples.dart';

Future<void> checkFeedback(WidgetTester wt) async {
  for (final rating in FeedbackRating.values) {
    for (final send in [true, false]) {
      await _checkFeedback(
        wt,
        rating: rating,
        send: send,
      );
    }
  }
}

Future<void> _checkFeedback(
  WidgetTester wt, {
  required FeedbackRating rating,
  required bool send,
}) async {
  await wt.tapAndSettle(find.feedbackThumb(rating));

  expectLastAnalyticsEvent(
    AppRatedAnalyticsEvent(
      rating: rating,
      snippetContext: defaultEventSnippetContext,
    ),
    reason: 'Rating: $rating, Send: $send',
  );
  expect(find.feedbackDropdownContent(), findsOneWidget);

  if (!send) {
    await wt.tapAndSettle(find.dismissibleOverlay());
  } else {
    final text = 'This is $rating text.';
    await wt.enterText(find.feedbackDropdownTextField(), text);
    await wt.pumpAndSettle();

    expect(find.text(text), findsOneWidget);

    await wt.tapAndSettle(find.feedbackDropdownSendButton());

    expectLastAnalyticsEvent(
      FeedbackFormSentAnalyticsEvent(
        rating: rating,
        text: text,
        snippetContext: defaultEventSnippetContext,
      ),
    );
  }

  expect(find.feedbackDropdownContent(), findsNothing);
}
