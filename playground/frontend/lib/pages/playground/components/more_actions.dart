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
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground/config/theme.dart';
import 'package:playground/constants/assets.dart';
import 'package:playground/modules/shortcuts/components/shortcuts_modal.dart';
import 'package:url_launcher/url_launcher.dart';

enum HeaderAction {
  shortcuts,
  beamPlaygroundGithub,
  apacheBeamGithub,
  scioGithub,
  beamWebsite,
  aboutBeam,
}

const kShortcutsText = 'Shortcuts';

const kBeamPlaygroundGithubText = 'Beam Playground on GitHub';
const kBeamPlaygroundGithubLink =
    'https://github.com/apache/beam/tree/master/playground/frontend';

const kApacheBeamGithubText = 'Apache Beam on GitHub';
const kApacheBeamGithubLink =
    'https://github.com/apache/beam/tree/master/playground/frontend';

const kScioGithubText = 'SCIO on GitHub';
const kScioGithubLink = 'https://github.com/spotify/scio';

const kBeamWebsiteText = 'To Apache Beam website';
const kBeamWebsiteLink = 'https://beam.apache.org/';

const kAboutBeamText = 'About Apache Beam';

class MoreActions extends StatefulWidget {
  const MoreActions({Key? key}) : super(key: key);

  @override
  State<MoreActions> createState() => _MoreActionsState();
}

class _MoreActionsState extends State<MoreActions> {
  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16.0, vertical: 8.0),
      child: PopupMenuButton<HeaderAction>(
        icon: Icon(
          Icons.more_horiz_outlined,
          color: ThemeColors.of(context).grey1Color,
        ),
        itemBuilder: (BuildContext context) => <PopupMenuEntry<HeaderAction>>[
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.shortcuts,
            child: ListTile(
              leading: SvgPicture.asset(kShortcutsIconAsset),
              title: const Text(kShortcutsText),
              onTap: () => {
                showDialog<void>(
                  context: context,
                  builder: (BuildContext context) => const ShortcutsModal(),
                )
              },
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamPlaygroundGithub,
            child: ListTile(
              leading: SvgPicture.asset(kGithubIconAsset),
              title: const Text(kBeamPlaygroundGithubText),
              onTap: () => launch(kBeamPlaygroundGithubLink),
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.apacheBeamGithub,
            child: ListTile(
              leading: SvgPicture.asset(kGithubIconAsset),
              title: const Text(kApacheBeamGithubText),
              onTap: () => launch(kApacheBeamGithubLink),
            ),
          ),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.scioGithub,
            child: ListTile(
              leading: SvgPicture.asset(kGithubIconAsset),
              title: const Text(kScioGithubText),
              onTap: () => launch(kScioGithubLink),
            ),
          ),
          const PopupMenuDivider(height: 16.0),
          PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamWebsite,
            child: ListTile(
              leading: const Image(image: AssetImage('beam.png')),
              title: const Text(kBeamWebsiteText),
              onTap: () => launch(kBeamWebsiteLink),
            ),
          ),
          const PopupMenuItem<HeaderAction>(
            padding: EdgeInsets.zero,
            value: HeaderAction.beamWebsite,
            child: ListTile(
              leading: Icon(Icons.info_outline),
              title: Text(kAboutBeamText),
            ),
          ),
        ],
      ),
    );
  }
}
