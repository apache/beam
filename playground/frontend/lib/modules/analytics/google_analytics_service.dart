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
import 'package:playground/modules/analytics/analytics_events.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground_components/playground_components.dart';
import 'package:usage/usage_html.dart';

class GoogleAnalyticsService implements AnalyticsService {
  final _analytics = AnalyticsHtml(kAnalyticsUA, 'beam', '1.0');

  @override
  AnalyticsEvent? lastSentEvent;

  @override
  void trackSelectSdk(Sdk? oldSdk, Sdk newSdk) {
    _safeSendEvent(AnalyticsEvent(
      action: kSelectSdkEvent,
      category: kSdkCategory,
      label: '${oldSdk?.title}_${newSdk.title}',
    ));
  }

  @override
  void trackSelectExample(ExampleBase newExample) {
    _safeSendEvent(AnalyticsEvent(
      action: kSelectExampleEvent,
      category: kExampleCategory,
      label: newExample.path,
    ));
  }

  @override
  void trackClickNewExample() {
    _safeSendEvent(AnalyticsEvent(
      action: kClickNewExampleEvent,
      category: kExampleCategory,
    ));
  }

  @override
  void trackReset() {
    _safeSendEvent(AnalyticsEvent(
      action: kClickResetEvent,
      category: kCommonCategory,
    ));
  }

  @override
  void trackClickToggleTheme(bool isDark) {
    _safeSendEvent(AnalyticsEvent(
      action: kClickToggleThemeEvent,
      category: kCommonCategory,
      label: isDark ? 'dark' : 'light',
    ));
  }

  @override
  void trackOpenShortcutsModal() {
    _safeSendEvent(AnalyticsEvent(
      action: kOpenShortcutsModalEvent,
      category: kCommonCategory,
    ));
  }

  @override
  void trackOpenLink(String link) {
    _safeSendEvent(AnalyticsEvent(
      action: kOpenLinkEvent,
      category: kLinkCategory,
      label: link,
    ));
  }

  @override
  void trackClickEnjoyPlayground(bool isEnjoying) {
    _safeSendEvent(AnalyticsEvent(
      action: kClickEnjoyPlaygroundEvent,
      category: kFeedbackCategory,
      label: 'isEnjoying = $isEnjoying',
    ));
  }

  @override
  void trackClickReportIssue() {
    _safeSendEvent(AnalyticsEvent(
      action: kClickReportIssueEvent,
      category: kFeedbackCategory,
    ));
  }

  @override
  void trackClickRunEvent(String exampleName) {
    _safeSendEvent(AnalyticsEvent(
      action: kClickRunEvent,
      category: kRunCodeCategory,
      label: exampleName,
    ));
  }

  @override
  void trackClickCancelRunEvent(String exampleName) {
    _safeSendEvent(AnalyticsEvent(
      action: kClickCancelRunEvent,
      category: kRunCodeCategory,
      label: exampleName,
    ));
  }

  @override
  void trackClickSendPositiveFeedback(String feedback) {
    _safeSendEvent(AnalyticsEvent(
      action: kClickSendPositiveFeedbackEvent,
      category: kFeedbackCategory,
      label: feedback,
    ));
  }

  @override
  void trackClickSendNegativeFeedback(String feedback) {
    _safeSendEvent(AnalyticsEvent(
      action: kClickSendNegativeFeedbackEvent,
      category: kFeedbackCategory,
      label: feedback,
    ));
  }

  @override
  void trackRunTimeEvent(String exampleName, int runTimeMs) {
    _safeSendEvent(AnalyticsEvent(
      action: kRunTimeEvent,
      category: kRunCodeCategory,
      label: exampleName,
      value: runTimeMs,
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
