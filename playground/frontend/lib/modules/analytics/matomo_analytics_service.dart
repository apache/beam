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

import 'package:matomo/matomo.dart';
import 'package:playground/config.g.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/analytics/analytics_events.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class MatomoAnalyticsService implements AnalyticsService {
  MatomoAnalyticsService() {
    MatomoTracker().initialize(
      siteId: kMatomoSiteId,
      url: kMatomoEndpointUrl,
    );
  }

  @override
  void trackSelectSdk(SDK oldSdk, SDK newSdk) {
    _sendEvent(
      category: kSdkCategory,
      action: kSelectSdkEvent,
      name: '${oldSdk.displayName}_${newSdk.displayName}',
    );
  }

  @override
  void trackSelectExample(ExampleModel newExample) {
    _sendEvent(
      category: kExampleCategory,
      action: kSelectExampleEvent,
      name: newExample.path,
    );
  }

  @override
  void trackClickNewExample() {
    _sendEvent(
      category: kExampleCategory,
      action: kClickNewExampleEvent,
      name: '',
    );
  }

  @override
  void trackReset() {
    _sendEvent(
      category: kCommonCategory,
      action: kClickResetEvent,
      name: '',
    );
  }

  @override
  void trackClickToggleTheme(bool isDark) {
    _sendEvent(
      category: kCommonCategory,
      action: kClickToggleThemeEvent,
      name: isDark ? 'dark' : 'light',
    );
  }

  @override
  void trackOpenShortcutsModal() {
    _sendEvent(
      category: kCommonCategory,
      action: kOpenShortcutsModalEvent,
      name: '',
    );
  }

  @override
  void trackOpenLink(String link) {
    _sendEvent(
      category: kLinkCategory,
      action: kOpenLinkEvent,
      name: link,
    );
  }

  @override
  void trackClickEnjoyPlayground(bool isEnjoying) {
    _sendEvent(
      category: kFeedbackCategory,
      action: kClickEnjoyPlaygroundEvent,
      name: 'isEnjoying = $isEnjoying',
    );
  }

  @override
  void trackClickReportIssue() {
    _sendEvent(
      category: kFeedbackCategory,
      action: kClickReportIssueEvent,
      name: '',
    );
  }

  @override
  void trackClickRunEvent(String exampleName) {
    _sendEvent(
      category: kRunCodeCategory,
      action: kClickRunEvent,
      name: exampleName,
    );
  }

  @override
  void trackClickCancelRunEvent(String exampleName) {
    _sendEvent(
      category: kRunCodeCategory,
      action: kClickCancelRunEvent,
      name: exampleName,
    );
  }

  @override
  void trackClickSendFeedback(String feedback) {
    _sendEvent(
      category: kFeedbackCategory,
      action: kClickSendFeedbackEvent,
      name: feedback,
    );
  }

  @override
  void trackRunTimeEvent(String exampleName, int runTimeMs) {
    _sendEvent(
      category: kRunCodeCategory,
      action: kRunTimeEvent,
      name: exampleName,
      value: runTimeMs,
    );
  }

  void _sendEvent({
    required String category,
    required String action,
    required String name,
    int? value,
  }) {
    MatomoTracker.trackEvent(
      name,
      action,
      widgetName: category,
      eventValue: value,
    );
  }
}
