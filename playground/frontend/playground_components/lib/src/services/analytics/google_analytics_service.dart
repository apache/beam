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

import 'package:firebase_analytics/firebase_analytics.dart';

import 'beam/categories.dart';
import 'beam/events.dart';
import 'event.dart';
import 'service.dart';

class BeamAnalyticsService implements AnalyticsService {
  late FirebaseAnalytics _appAnalytics;

  // TODO(nausharipov): review: removed usage package to make test tasks work.
  BeamAnalyticsService();

  @override
  AnalyticsEvent? lastSentEvent;

  @override
  Future<void> trackRunExample(String exampleName) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.run,
        category: BeamAnalyticsCategories.run,
        label: exampleName,
      ),
    );
  }

  @override
  Future<void> trackModifyExample(String exampleName) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.modifySnippet,
        category: BeamAnalyticsCategories.example,
        label: exampleName,
      ),
    );
  }

  @override
  Future<void> trackClickSendPositiveFeedback(String feedback) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.clickSendPositiveFeedback,
        category: BeamAnalyticsCategories.feedback,
        label: feedback,
      ),
    );
  }

  @override
  Future<void> trackClickSendNegativeFeedback(String feedback) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.clickSendNegativeFeedback,
        category: BeamAnalyticsCategories.feedback,
        label: feedback,
      ),
    );
  }

  @override
  Future<void> trackClickReportIssue() async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.clickReportIssue,
        category: BeamAnalyticsCategories.feedback,
      ),
    );
  }

  @override
  Future<void> trackClickCancelRunEvent(String exampleName) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.clickCancelRun,
        category: BeamAnalyticsCategories.run,
        label: exampleName,
      ),
    );
  }

  @override
  Future<void> trackRunTimeEvent(String exampleName, int runTimeMs) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.runTime,
        category: BeamAnalyticsCategories.run,
        label: exampleName,
        value: runTimeMs,
      ),
    );
  }

  @override
  Future<void> trackReset() async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.clickReset,
        category: BeamAnalyticsCategories.common,
      ),
    );
  }

  @override
  Future<void> trackClickToggleTheme({required bool isDark}) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.clickToggleTheme,
        category: BeamAnalyticsCategories.common,
        label: isDark ? 'dark' : 'light',
      ),
    );
  }

  @override
  Future<void> trackOpenLink(String link) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: BeamAnalyticsEvents.openLink,
        category: BeamAnalyticsCategories.link,
        label: link,
      ),
    );
  }

  Future<void> _safeSendEvent(AnalyticsEvent analyticsEvent) async {
    try {
      await _appAnalytics.logEvent(
        name: analyticsEvent.action,
      );
      lastSentEvent = analyticsEvent;
    } on Exception catch (e) {
      // ignore analytics errors sync they don't affect app
      print(['GoogleAnalyticsService _safeSendEvent error: ', e]);
    }
  }
}
