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
import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter/material.dart';
import 'package:flutter_svg/svg.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

import '../../assets/assets.gen.dart';
import '../../auth/notifier.dart';
import '../../constants/sizes.dart';

class UserMenu extends StatelessWidget {
  final VoidCallback closeOverlayCallback;
  final User user;

  const UserMenu({
    required this.closeOverlayCallback,
    required this.user,
  });

  @override
  Widget build(BuildContext context) {
    return OverlayBody(
      child: SizedBox(
        width: TobSizes.authOverlayWidth,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            _Info(user: user),
            const BeamDivider(),
            _Buttons(
              closeOverlayCallback: closeOverlayCallback,
            ),
          ],
        ),
      ),
    );
  }
}

class _Info extends StatelessWidget {
  final User user;

  const _Info({
    required this.user,
  });

  @override
  Widget build(BuildContext context) {
    final displayName = user.displayName;
    final email = user.email;

    return Padding(
      padding: const EdgeInsets.all(BeamSizes.size16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          if (displayName != null)
            Text(
              displayName,
              style: Theme.of(context).textTheme.titleLarge,
            ),
          if (email != null)
            Text(
              email,
              style: Theme.of(context).textTheme.bodySmall,
            ),
        ],
      ),
    );
  }
}

class _Buttons extends StatelessWidget {
  final VoidCallback closeOverlayCallback;

  const _Buttons({
    required this.closeOverlayCallback,
  });

  @override
  Widget build(BuildContext context) {
    final authNotifier = GetIt.instance.get<AuthNotifier>();

    return Column(
      children: [
        _IconLabel(
          isSvg: false,
          onTap: () {
            unawaited(launchUrl(Uri.parse(BeamLinks.website)));
          },
          iconPath: Assets.png.profileWebsite.path,
          label: 'ui.toWebsite'.tr(),
        ),
        const BeamDivider(),
        _IconLabel(
          onTap: () async {
            await authNotifier.logOut();
            closeOverlayCallback();
          },
          iconPath: Assets.svg.profileLogout,
          label: 'ui.signOut'.tr(),
        ),
        const BeamDivider(),
        _IconLabel(
          onTap: () async {
            closeOverlayCallback();
            final confirmed = await ConfirmDialog.show(
              context: context,
              confirmButtonText: 'ui.deleteMyAccount'.tr(),
              subtitle: 'dialogs.deleteAccountWarning'.tr(),
              title: 'ui.deleteTobAccount'.tr(),
            );
            if (confirmed) {
              ProgressDialog.show(
                future: authNotifier.deleteAccount(),
                navigatorKey:
                    GetIt.instance.get<BeamRouterDelegate>().navigatorKey!,
              );
            }
          },
          iconPath: Assets.svg.profileDelete,
          label: 'ui.deleteMyAccount'.tr(),
        ),
      ],
    );
  }
}

class _IconLabel extends StatelessWidget {
  final String iconPath;
  final String label;
  final VoidCallback? onTap;

  // TODO(alexeyinkin): Auto-determine.
  final bool isSvg;

  const _IconLabel({
    required this.iconPath,
    required this.label,
    required this.onTap,
    this.isSvg = true,
  });

  @override
  Widget build(BuildContext context) {
    return InkWell(
      onTap: onTap,
      child: Padding(
        padding: const EdgeInsets.all(BeamSizes.size12),
        child: Row(
          children: [
            if (isSvg)
              SvgPicture.asset(iconPath)
            else
              Image.asset(
                iconPath,
                height: 20,
              ),
            const SizedBox(width: BeamSizes.size10),
            Text(label),
          ],
        ),
      ),
    );
  }
}
