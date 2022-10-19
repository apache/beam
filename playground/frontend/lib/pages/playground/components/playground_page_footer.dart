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
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/links.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/pages/playground/components/feedback/playground_feedback.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

class PlaygroundPageFooter extends StatelessWidget {
  const PlaygroundPageFooter({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    return Container(
      color: Theme.of(context)
          .extension<BeamThemeExtension>()
          ?.secondaryBackgroundColor,
      width: double.infinity,
      child: Padding(
        padding: const EdgeInsets.symmetric(
          vertical: kSmSpacing,
          horizontal: kXlSpacing,
        ),
        child: Wrap(
          spacing: kXlSpacing,
          crossAxisAlignment: WrapCrossAlignment.center,
          children: [
            const PlaygroundFeedback(),
            TextButton(
              style: TextButton.styleFrom(
                textStyle: const TextStyle(fontWeight: kNormalWeight),
              ),
              onPressed: () {
                launchUrl(Uri.parse(kReportIssueLink));
                AnalyticsService.get(context).trackClickReportIssue();
              },
              child: Text(appLocale.reportIssue),
            ),
            TextButton(
              style: TextButton.styleFrom(
                textStyle: const TextStyle(fontWeight: kNormalWeight),
              ),
              onPressed: () {
                AnalyticsService.get(context)
                    .trackOpenLink(kBeamPrivacyPolicyLink);
                launchUrl(Uri.parse(kBeamPrivacyPolicyLink));
              },
              child: Text(appLocale.privacyPolicy),
            ),
            Text(appLocale.copyright),
          ],
        ),
      ),
    );
  }
}
