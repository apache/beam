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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground/config/theme.dart';
import 'package:playground/constants/assets.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:provider/provider.dart';

class ToggleThemeButton extends StatelessWidget {
  const ToggleThemeButton({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final appLocale = AppLocalizations.of(context)!;

    return Consumer<ThemeSwitchNotifier>(builder: (context, notifier, child) {
      final text = notifier.isDarkMode ? appLocale.lightMode : appLocale.darkMode;

      return Padding(
        padding: const EdgeInsets.symmetric(
          vertical: kSmSpacing,
          horizontal: kMdSpacing,
        ),
        child: TextButton.icon(
          icon: SvgPicture.asset(kThemeIconAsset),
          label: Text(text),
          onPressed: () {
            notifier.toggleTheme();
            AnalyticsService.get(context)
                .trackClickToggleTheme(!notifier.isDarkMode);
          },
        ),
      );
    });
  }
}
