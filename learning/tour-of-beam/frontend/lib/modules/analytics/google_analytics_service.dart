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

import 'package:playground_components/playground_components.dart';
import 'package:usage/usage_html.dart';

import '../../config.dart';
import 'categories.dart';
import 'events.dart';
import 'service.dart';

class TobGoogleAnalyticsService extends GoogleAnalyticsService
    implements TobAnalyticsService {
  static const _appName = 'beam';
  static const _appVersion = '1.0';
  final _analytics = AnalyticsHtml(kAnalyticsUA, _appName, _appVersion);

  TobGoogleAnalyticsService()
      : super(
          appAnalytics: AnalyticsHtml(
            kAnalyticsUA,
            _appName,
            _appVersion,
          ),
        );

  @override
  Future<void> trackOpenUnit(Sdk? oldSdk, Sdk newSdk) async {
    await _safeSendEvent(
      AnalyticsEvent(
        action: TobAnalyticsEvents.openUnit,
        category: TobAnalyticsCategories.sdk,
        label: '${oldSdk?.title}_${newSdk.title}',
      ),
    );
  }

  Future<void> _safeSendEvent(AnalyticsEvent analyticsEvent) async {
    try {
      await _analytics.sendEvent(
        analyticsEvent.category,
        analyticsEvent.action,
        label: analyticsEvent.label,
        value: analyticsEvent.value,
        parameters: analyticsEvent.parameters,
      );
      lastSentEvent = analyticsEvent;
    } on Exception catch (e) {
      // ignore analytics errors sync they don't affect app
      print(['TobGoogleAnalyticsService _safeSendEvent error: ', e]);
    }
  }
}
