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

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:url_launcher/url_launcher.dart';

import '../../services/analytics/analytics_service.dart';
import '../../services/analytics/events/external_url_navigated.dart';
import 'text.dart';

/// A [BeamTextButton] that navigates [url] and fires
/// an [ExternalUrlNavigatedAnalyticsEvent].
class TextExternalUrlNavigationButton extends StatelessWidget {
  const TextExternalUrlNavigationButton({
    required this.title,
    required this.url,
  });

  final String title;
  final Uri url;

  @override
  Widget build(BuildContext context) {
    return BeamTextButton(
      onPressed: () {
        GetIt.instance.get<BeamAnalyticsService>().sendUnawaited(
          ExternalUrlNavigatedAnalyticsEvent(url: url),
        );
        unawaited(launchUrl(url));
      },
      title: title,
    );
  }
}
