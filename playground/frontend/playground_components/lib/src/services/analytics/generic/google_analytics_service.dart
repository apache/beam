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

import 'package:usage/usage_html.dart';

import '../event.dart';
import 'categories.dart';
import 'events.dart';
import 'service.dart';

class GenericGoogleAnalyticsService implements GenericAnalyticsService {
  late AnalyticsHtml _appAnalytics;

  GenericGoogleAnalyticsService({
    required AnalyticsHtml appAnalytics,
  }) {
    _appAnalytics = appAnalytics;
  }

  @override
  AnalyticsEvent? lastSentEvent;

  @override
  Future<void> trackRunExample(String exampleName) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: GenericAnalyticsEvents.run,
        category: GenericAnalyticsCategories.run,
        label: exampleName,
      ),
    );
  }

  Future<void> _safeSendEvent(AnalyticsEvent analyticsEvent) async {
    try {
      await _appAnalytics.sendEvent(
        analyticsEvent.category,
        analyticsEvent.action,
        label: analyticsEvent.label,
        value: analyticsEvent.value,
        parameters: analyticsEvent.parameters,
      );
      lastSentEvent = analyticsEvent;
    } on Exception catch (e) {
      // ignore analytics errors sync they don't affect app
      print(['GoogleAnalyticsService _safeSendEvent error: ', e]);
    }
  }
}
