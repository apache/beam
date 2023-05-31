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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

import '../../../modules/shortcuts/components/shortcuts_dialog.dart';
import '../../../services/analytics/events/shortcuts_clicked.dart';
import '../../../src/assets/assets.gen.dart';

enum HeaderAction {
  versions,
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
          //
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.versions,
            child: ListTile(
              leading: const Icon(Icons.watch_later_outlined),
              title: const Text('widgets.versions.title').tr(),
              onTap: () => BeamDialog.show(
                context: context,
                title: const Text('widgets.versions.title').tr(),
                child: const VersionsWidget(sdks: Sdk.known),
              ),
            ),
          ),

          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.shortcuts,
            child: ListTile(
              leading: SvgPicture.asset(Assets.shortcuts),
              title: Text(appLocale.shortcuts),
              onTap: () {
                Navigator.of(context).pop();
                PlaygroundComponents.analyticsService.sendUnawaited(
                  const ShortcutsClickedAnalyticsEvent(),
                );
                BeamDialog.show(
                  actions: [BeamCloseButton()],
                  context: context,
                  title: Text(appLocale.shortcuts),
                  child: ShortcutsDialogContent(
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
              leading: SvgPicture.asset(Assets.github),
              title: Text(appLocale.beamPlaygroundOnGithub),
              onTap: () => _openLink(BeamLinks.playgroundGitHub, context),
            ),
          ),

          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.apacheBeamGithub,
            child: ListTile(
              leading: SvgPicture.asset(Assets.github),
              title: Text(appLocale.apacheBeamOnGithub),
              onTap: () => _openLink(BeamLinks.github, context),
            ),
          ),

          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.scioGithub,
            child: ListTile(
              leading: SvgPicture.asset(Assets.github),
              title: Text(appLocale.scioOnGithub),
              onTap: () => _openLink(BeamLinks.scioGitHub, context),
            ),
          ),

          const PopupMenuDivider(height: 16.0),

          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamWebsite,
            child: ListTile(
              leading: Image(image: AssetImage(Assets.beam.path)),
              title: Text(appLocale.toApacheBeamWebsite),
              onTap: () => _openLink(BeamLinks.website, context),
            ),
          ),

          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.aboutBeam,
            child: ListTile(
              leading: const Icon(Icons.info_outline),
              title: Text(appLocale.aboutApacheBeam),
              onTap: () => _openLink(BeamLinks.about, context),
            ),
          ),
        ],
      ),
    );
  }

  void _openLink(String link, BuildContext context) {
    final url = Uri.parse(link);

    Navigator.of(context).pop();
    launchUrl(url);
    PlaygroundComponents.analyticsService.sendUnawaited(
      ExternalUrlNavigatedAnalyticsEvent(url: url),
    );
  }
}
