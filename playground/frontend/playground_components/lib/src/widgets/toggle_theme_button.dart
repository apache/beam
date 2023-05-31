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
import 'package:flutter_svg/svg.dart';
import 'package:provider/provider.dart';

import '../assets/assets.gen.dart';
import '../playground_components.dart';
import '../services/analytics/events/theme_set.dart';
import '../theme/switch_notifier.dart';

class ToggleThemeButton extends StatelessWidget {
  const ToggleThemeButton();

  @override
  Widget build(BuildContext context) {
    return Consumer<ThemeSwitchNotifier>(
      builder: (context, notifier, child) {
        final text =
            notifier.isDarkMode ? 'ui.lightMode'.tr() : 'ui.darkMode'.tr();

        return TextButton.icon(
          icon: SvgPicture.asset(
            Assets.buttons.themeMode,
            package: PlaygroundComponents.packageName,
          ),
          label: Text(text),
          onPressed: () {
            unawaited(notifier.toggleTheme());
            PlaygroundComponents.analyticsService.sendUnawaited(
              ThemeSetAnalyticsEvent(
                brightness: notifier.brightness,
              ),
            );
          },
        );
      },
    );
  }
}
