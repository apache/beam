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
import 'package:playground/constants/assets.dart';
import 'package:playground/constants/links.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/shortcuts/components/shortcuts_modal.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

enum HeaderAction {
  shortcuts,
  beamPlaygroundGithub,
  apacheBeamGithub,
  scioGithub,
  beamWebsite,
  aboutBeam,
}

class MoreActions extends StatefulWidget {
  final PlaygroundController playgroundController;

  const MoreActions({
    required this.playgroundController,
  });

  @override
  State<MoreActions> createState() => _MoreActionsState();
}

class _MoreActionsState extends State<MoreActions> {
  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16.0, vertical: 8.0),
      child: PopupMenuButton<HeaderAction>(
        icon: Icon(
          Icons.more_horiz_outlined,
          color: Theme.of(context).extension<BeamThemeExtension>()?.iconColor,
        ),
        itemBuilder: (BuildContext context) => <PopupMenuEntry<HeaderAction>>[
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.shortcuts,
            child: ListTile(
              leading: SvgPicture.asset(kShortcutsIconAsset),
              title: Text(appLocale.shortcuts),
              onTap: () {
                AnalyticsService.get(context).trackOpenShortcutsModal();
                showDialog<void>(
                  context: context,
                  builder: (BuildContext context) => ShortcutsModal(
                    playgroundController: widget.playgroundController,
                  ),
                );
              },
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamPlaygroundGithub,
            child: ListTile(
              leading: SvgPicture.asset(kGithubIconAsset),
              title: Text(appLocale.beamPlaygroundOnGithub),
              onTap: () => _openLink(kBeamPlaygroundGithubLink, context),
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.apacheBeamGithub,
            child: ListTile(
              leading: SvgPicture.asset(kGithubIconAsset),
              title: Text(appLocale.apacheBeamOnGithub),
              onTap: () => _openLink(kApacheBeamGithubLink, context),
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.scioGithub,
            child: ListTile(
              leading: SvgPicture.asset(kGithubIconAsset),
              title: Text(appLocale.scioOnGithub),
              onTap: () => _openLink(kScioGithubLink, context),
            ),
          ),
          const PopupMenuDivider(height: 16.0),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamWebsite,
            child: ListTile(
              leading: const Image(image: AssetImage(kBeamIconAsset)),
              title: Text(appLocale.toApacheBeamWebsite),
              onTap: () => _openLink(kBeamWebsiteLink, context),
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamWebsite,
            child: ListTile(
              leading: const Icon(Icons.info_outline),
              title: Text(appLocale.aboutApacheBeam),
              onTap: () => _openLink(kAboutBeamLink, context),
            ),
          ),
        ],
      ),
    );
  }

  _openLink(String link, BuildContext context) {
    launchUrl(Uri.parse(link));
    AnalyticsService.get(context).trackOpenLink(link);
  }
}
