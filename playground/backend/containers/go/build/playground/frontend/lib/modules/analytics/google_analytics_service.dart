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
  void trackSelectSdk(Sdk? oldSdk, Sdk newSdk) {
    safeSendEvent(
      kSdkCategory,
      kSelectSdkEvent,
      label: '${oldSdk?.title}_${newSdk.title}',
    );
  }

  @override
  void trackSelectExample(ExampleBase newExample) {
    safeSendEvent(
      kExampleCategory,
      kSelectExampleEvent,
      label: newExample.path,
    );
  }

  @override
  void trackClickNewExample() {
    safeSendEvent(kExampleCategory, kClickNewExampleEvent);
  }

  @override
  void trackReset() {
    safeSendEvent(kCommonCategory, kClickResetEvent);
  }

  @override
  void trackClickToggleTheme(bool isDark) {
    safeSendEvent(
      kCommonCategory,
      kClickToggleThemeEvent,
      label: isDark ? 'dark' : 'light',
    );
  }

  @override
  void trackOpenShortcutsModal() {
    safeSendEvent(kCommonCategory, kOpenShortcutsModalEvent);
  }

  @override
  void trackOpenLink(String link) {
    safeSendEvent(
      kLinkCategory,
      kOpenLinkEvent,
      label: link,
    );
  }

  @override
  void trackClickEnjoyPlayground(bool isEnjoying) {
    safeSendEvent(
      kFeedbackCategory,
      kClickEnjoyPlaygroundEvent,
      label: 'isEnjoying = $isEnjoying',
    );
  }

  @override
  void trackClickReportIssue() {
    safeSendEvent(kFeedbackCategory, kClickReportIssueEvent);
  }

  @override
  void trackClickRunEvent(String exampleName) {
    safeSendEvent(
      kRunCodeCategory,
      kClickRunEvent,
      label: exampleName,
    );
  }

  @override
  void trackClickCancelRunEvent(String exampleName) {
    safeSendEvent(
      kRunCodeCategory,
      kClickCancelRunEvent,
      label: exampleName,
    );
  }

  @override
  void trackClickSendFeedback(String feedback) {
    safeSendEvent(
      kFeedbackCategory,
      kClickSendFeedbackEvent,
      label: feedback,
    );
  }

  @override
  void trackRunTimeEvent(String exampleName, int runTimeMs) {
    safeSendEvent(
      kRunCodeCategory,
      kRunTimeEvent,
      label: exampleName,
      value: runTimeMs,
    );
  }

  void safeSendEvent(String category, String action,
      {String? label, int? value, Map<String, String>? parameters}) {
    try {
      _analytics.sendEvent(
        category,
        action,
        label: label,
        value: value,
        parameters: parameters,
      );
    } catch (e) {
      // ignore analytics errors sync they don't affect app
      print(e);
    }
  }
}
