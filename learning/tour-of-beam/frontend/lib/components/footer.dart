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
import 'package:url_launcher/url_launcher.dart';

import '../config/theme/colors_provider.dart';
import '../constants/links.dart';
import '../constants/sizes.dart';

class Footer extends StatelessWidget {
  const Footer();

  @override
  Widget build(BuildContext context) {
    return _Body(
      child: Wrap(
        spacing: TobSizes.size16,
        crossAxisAlignment: WrapCrossAlignment.center,
        children: [
          const _ReportIssueButton(),
          const _PrivacyPolicyButton(),
          const Text('ui.copyright').tr(),
        ],
      ),
    );
  }
}

class _Body extends StatelessWidget {
  final Widget child;
  const _Body({required this.child});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(
        vertical: TobSizes.size4,
        horizontal: TobSizes.size16,
      ),
      decoration: BoxDecoration(
        color: ThemeColors.of(context).secondaryBackground,
        border: Border(
          top: BorderSide(color: ThemeColors.of(context).divider),
        ),
      ),
      height: TobSizes.footerHeight,
      width: double.infinity,
      child: child,
    );
  }
}

class _ReportIssueButton extends StatelessWidget {
  const _ReportIssueButton();

  @override
  Widget build(BuildContext context) {
    return TextButton(
      style: _linkButtonStyle,
      onPressed: () {
        launchUrl(Uri.parse(TobLinks.reportIssue));
      },
      child: const Text('ui.reportIssue').tr(),
    );
  }
}

class _PrivacyPolicyButton extends StatelessWidget {
  const _PrivacyPolicyButton();

  @override
  Widget build(BuildContext context) {
    return TextButton(
      style: _linkButtonStyle,
      onPressed: () {
        launchUrl(Uri.parse(TobLinks.privacyPolicy));
      },
      child: const Text('ui.privacyPolicy').tr(),
    );
  }
}

final _linkButtonStyle = TextButton.styleFrom(
  textStyle: const TextStyle(
    fontSize: 12,
    fontWeight: FontWeight.w400,
  ),
);
