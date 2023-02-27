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
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';

import '../../../constants/font_weight.dart';
import '../../../constants/links.dart';
import '../../../constants/sizes.dart';
import 'feedback/playground_feedback.dart';

class PlaygroundPageFooter extends StatelessWidget {
  const PlaygroundPageFooter({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    return Consumer<PlaygroundController>(
      builder: (context, playgroundController, child) => Container(
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
                  PlaygroundComponents.analyticsService.sendUnawaited(
                    ReportIssueClickedAnalyticsEvent(
                      snippetContext: playgroundController.eventSnippetContext,
                    ),
                  );
                },
                child: Text(appLocale.reportIssue),
              ),
              TextButton(
                style: TextButton.styleFrom(
                  textStyle: const TextStyle(fontWeight: kNormalWeight),
                ),
                onPressed: () {
                  final url = Uri.parse(kBeamPrivacyPolicyLink);
                  launchUrl(url);
                  PlaygroundComponents.analyticsService.sendUnawaited(
                    ExternalUrlNavigatedAnalyticsEvent(url: url),
                  );
                },
                child: Text(appLocale.privacyPolicy),
              ),
              Text(appLocale.copyright),
            ],
          ),
        ),
      ),
    );
  }
}
