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

import 'package:flutter/material.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/components/link_button/github_button.dart';
import 'package:playground/pages/standalone_playground/widgets/more_actions.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import '../common/common_finders.dart';

Future<void> checkExternalUrlNavigation(WidgetTester wt) async {
  await _checkBottomBar(wt);
  await _checkMenuItems(wt);
  await _checkExampleDescription(wt);
}

Future<void> _checkBottomBar(WidgetTester wt) async {
  await _tapAndExpectNavigationEvent(
    wt,
    [
      () => find.byType(PrivacyPolicyButton),
    ],
    BeamLinks.privacyPolicy,
  );
}

Future<void> _checkMenuItems(WidgetTester wt) async {
  await _tapAndExpectNavigationEvent(
    wt,
    [
      find.moreActions,
      () => find.menuItem(HeaderAction.beamPlaygroundGithub),
    ],
    BeamLinks.playgroundGitHub,
  );

  await _tapAndExpectNavigationEvent(
    wt,
    [
      find.moreActions,
      () => find.menuItem(HeaderAction.apacheBeamGithub),
    ],
    BeamLinks.github,
  );

  await _tapAndExpectNavigationEvent(
    wt,
    [
      find.moreActions,
      () => find.menuItem(HeaderAction.scioGithub),
    ],
    BeamLinks.scioGitHub,
  );

  await _tapAndExpectNavigationEvent(
    wt,
    [
      find.moreActions,
      () => find.menuItem(HeaderAction.beamWebsite),
    ],
    BeamLinks.website,
  );

  await _tapAndExpectNavigationEvent(
    wt,
    [
      find.moreActions,
      () => find.menuItem(HeaderAction.aboutBeam),
    ],
    BeamLinks.about,
  );
}

Future<void> _checkExampleDescription(WidgetTester wt) async {
  final url = wt.findPlaygroundController().selectedExample?.urlVcs;
  expect(url, isNotNull);

  await _tapAndExpectNavigationEvent(
    wt,
    [
      find.descriptionPopoverButton,
      () => find.descendant(
            of: find.descriptionPopover(),
            matching: find.byType(GithubButton),
          ),
    ],
    url!,
  );
}

/// Taps each element found with [tapGetters], then verifies the last
/// analytics event.
Future<void> _tapAndExpectNavigationEvent(
  WidgetTester wt,
  List<ValueGetter<Finder>> tapGetters,
  String url,
) async {
  for (final tapGetter in tapGetters) {
    await wt.tapAndSettle(tapGetter());
  }

  expectLastAnalyticsEvent(
    ExternalUrlNavigatedAnalyticsEvent(url: Uri.parse(url)),
    reason: url,
  );
}
