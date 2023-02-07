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

import 'package:playground/config.g.dart';
import 'package:playground/modules/analytics/events.dart';
import 'package:playground/modules/analytics/service.dart';
import 'package:playground/pages/standalone_playground/widgets/feedback/rating_enum.dart';
import 'package:playground_components/playground_components.dart';
import 'package:usage/usage_html.dart';

class PlaygroundGoogleAnalyticsService extends BeamAnalyticsService
    implements PlaygroundAnalyticsService {
  static const _appName = 'beam';
  static const _appVersion = '1.0';
  final _analytics = AnalyticsHtml(kAnalyticsUA, _appName, _appVersion);

// TODO(nausharipov): review: removed usage package to make test tasks work.
  PlaygroundGoogleAnalyticsService();

  @override
  void trackSelectSdk(Sdk? oldSdk, Sdk newSdk) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.selectSdk,
      category: BeamAnalyticsCategories.sdk,
      label: '${oldSdk?.title}_${newSdk.title}',
    ));
  }

  @override
  void trackSelectExample(ExampleBase newExample) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.selectExample,
      category: BeamAnalyticsCategories.example,
      label: newExample.path,
    ));
  }

  @override
  void trackClickNewExample() {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickNewExample,
      category: BeamAnalyticsCategories.example,
    ));
  }

  @override
  void trackOpenShortcutsModal() {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.openShortcutsModal,
      category: BeamAnalyticsCategories.common,
    ));
  }

  @override
  void trackClickEnjoyPlayground(FeedbackRating feedbackRating) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickEnjoyPlayground,
      category: BeamAnalyticsCategories.feedback,
      label: 'isEnjoying = ${feedbackRating.name}',
    ));
  }

  void _safeSendEvent(AnalyticsEvent analyticsEvent) {
    try {
      _analytics.sendEvent(
        analyticsEvent.category,
        analyticsEvent.action,
        label: analyticsEvent.label,
        value: analyticsEvent.value,
        parameters: analyticsEvent.parameters,
      );
      lastSentEvent = analyticsEvent;
    } catch (e) {
      // ignore analytics errors sync they don't affect app
      print(e);
    }
  }
}
