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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:playground/modules/shortcuts/constants/global_shortcuts.dart';
import 'package:playground/services/analytics/events/new_example.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

class NewExampleButton extends StatelessWidget {
  const NewExampleButton();

  @override
  Widget build(BuildContext context) {
    return ShortcutTooltip(
      shortcut: kNewExampleShortcut,
      child: HeaderIconButton(
        icon: Icon(
          Icons.add_circle_outline,
          color: Theme.of(context).extension<BeamThemeExtension>()?.iconColor,
        ),
        label: 'intents.playground.newExample'.tr(),
        onPressed: () {
          PlaygroundComponents.analyticsService.sendUnawaited(
            const NewExampleAnalyticsEvent(),
          );
          unawaited(launchUrl(Uri.parse(BeamLinks.newExample)));
        },
      ),
    );
  }
}
