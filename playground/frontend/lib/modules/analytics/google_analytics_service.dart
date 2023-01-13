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
import 'package:playground/modules/analytics/categories.dart';
import 'package:playground/modules/analytics/events.dart';
import 'package:playground/modules/analytics/service.dart';
import 'package:playground_components/playground_components.dart';
import 'package:usage/usage_html.dart';

class PlaygroundGoogleAnalyticsService extends GoogleAnalyticsService
    implements PlaygroundAnalyticsService {
  static const _appName = 'beam';
  static const _appVersion = '1.0';
  final _analytics = AnalyticsHtml(kAnalyticsUA, _appName, _appVersion);

  PlaygroundGoogleAnalyticsService()
      : super(
          appAnalytics: AnalyticsHtml(
            kAnalyticsUA,
            _appName,
            _appVersion,
          ),
        );

  @override
  void trackSelectSdk(Sdk? oldSdk, Sdk newSdk) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.selectSdk,
      category: AnalyticsCategories.sdk,
      label: '${oldSdk?.title}_${newSdk.title}',
    ));
  }

  @override
  void trackSelectExample(ExampleBase newExample) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.selectExample,
      category: AnalyticsCategories.example,
      label: newExample.path,
    ));
  }

  @override
  void trackClickNewExample() {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickNewExample,
      category: AnalyticsCategories.example,
    ));
  }

  @override
  void trackReset() {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickReset,
      category: AnalyticsCategories.common,
    ));
  }

  @override
  void trackClickToggleTheme(bool isDark) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickToggleTheme,
      category: AnalyticsCategories.common,
      label: isDark ? 'dark' : 'light',
    ));
  }

  @override
  void trackOpenShortcutsModal() {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.openShortcutsModal,
      category: AnalyticsCategories.common,
    ));
  }

  @override
  void trackOpenLink(String link) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.openLink,
      category: AnalyticsCategories.link,
      label: link,
    ));
  }

  @override
  void trackClickEnjoyPlayground(bool isEnjoying) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickEnjoyPlayground,
      category: AnalyticsCategories.feedback,
      label: 'isEnjoying = $isEnjoying',
    ));
  }

  @override
  void trackClickReportIssue() {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickReportIssue,
      category: AnalyticsCategories.feedback,
    ));
  }

  @override
  void trackClickRunEvent(String exampleName) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickRun,
      category: AnalyticsCategories.runCode,
      label: exampleName,
    ));
  }

  @override
  void trackClickCancelRunEvent(String exampleName) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickCancelRun,
      category: AnalyticsCategories.runCode,
      label: exampleName,
    ));
  }

  @override
  void trackClickSendPositiveFeedback(String feedback) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickSendPositiveFeedback,
      category: AnalyticsCategories.feedback,
      label: feedback,
    ));
  }

  @override
  void trackClickSendNegativeFeedback(String feedback) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.clickSendNegativeFeedback,
      category: AnalyticsCategories.feedback,
      label: feedback,
    ));
  }

  @override
  void trackRunTimeEvent(String exampleName, int runTimeMs) {
    _safeSendEvent(AnalyticsEvent(
      action: AnalyticsEvents.runTime,
      category: AnalyticsCategories.runCode,
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
