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
import 'package:playground/config.g.dart';
import 'package:playground/modules/analytics/analytics_events.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:provider/provider.dart';
import 'package:usage/usage_html.dart';

class AnalyticsService {
  late final Analytics _analytics;

  AnalyticsService() {
    _analytics = AnalyticsHtml(kAnalyticsUA, 'beam', '1.0');
  }

  static get(BuildContext context) {
    return Provider.of<AnalyticsService>(context, listen: false);
  }

  void trackSelectSdk(SDK oldSdk, SDK newSdk) {
    safeSendEvent(
      kSdkCategory,
      kSelectSdkEvent,
      label: '${oldSdk.displayName}_${newSdk.displayName}',
    );
  }

  void trackSelectExample(ExampleModel newExample) {
    safeSendEvent(
      kExampleCategory,
      kSelectExampleEvent,
      label: newExample.path,
    );
  }

  void trackClickNewExample() {
    safeSendEvent(kExampleCategory, kClickNewExampleEvent);
  }

  void trackReset() {
    safeSendEvent(kCommonCategory, kClickResetEvent);
  }

  void trackClickToggleTheme(bool isDark) {
    safeSendEvent(
      kCommonCategory,
      kClickToggleThemeEvent,
      label: isDark ? 'dark' : 'light',
    );
  }

  void trackOpenShortcutsModal() {
    safeSendEvent(kCommonCategory, kOpenShortcutsModalEvent);
  }

  void trackOpenLink(String link) {
    safeSendEvent(
      kLinkCategory,
      kOpenShortcutsModalEvent,
      label: link,
    );
  }

  void trackClickEnjoyPlayground(bool isEnjoying) {
    safeSendEvent(
      kFeedbackCategory,
      kClickEnjoyPlaygroundEvent,
      label: 'isEnjoying = ' + isEnjoying.toString(),
    );
  }

  void trackClickReportIssue() {
    safeSendEvent(kFeedbackCategory, kClickReportIssueEvent);
  }

  void trackClickRunEvent(ExampleModel? example) {
    safeSendEvent(
      kRunCodeCategory,
      kClickRunEvent,
      label: example?.path ?? '',
    );
  }

  void trackClickSendFeedback(String feedback) {
    safeSendEvent(
      kFeedbackCategory,
      kClickSendFeedbackEvent,
      label: feedback,
    );
  }

  void safeSendEvent(String category, String action, {String? label}) {
    try {
      _analytics.sendEvent(
        category,
        action,
        label: label,
      );
    } catch (e) {
      // ignore analytics errors sync they don't affect app
      print(e);
    }
  }
}
