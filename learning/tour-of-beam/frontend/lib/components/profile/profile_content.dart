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
import 'package:flutter_svg/svg.dart';
import 'package:playground_components/playground_components.dart';

import '../../constants/sizes.dart';
import '../../generated/assets.gen.dart';

class ProfileContent extends StatelessWidget {
  const ProfileContent();

  @override
  Widget build(BuildContext context) {
    return _Body(
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: const [
          _Info(),
          BeamDivider(),
          _Buttons(),
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
    return Material(
      elevation: BeamSizes.size10,
      borderRadius: BorderRadius.circular(10),
      child: SizedBox(
        width: TobSizes.authOverlayWidth,
        child: child,
      ),
    );
  }
}

class _Info extends StatelessWidget {
  const _Info();

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(BeamSizes.size16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            'Name Surname',
            style: Theme.of(context).textTheme.titleLarge,
          ),
          Text(
            'email@mail.com',
            style: Theme.of(context).textTheme.bodySmall,
          ),
        ],
      ),
    );
  }
}

class _Buttons extends StatelessWidget {
  const _Buttons();

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        _IconLabel(
          isSvg: false,
          onTap: () {},
          iconPath: Assets.png.profileWebsite.path,
          label: 'ui.toWebsite'.tr(),
        ),
        const BeamDivider(),
        _IconLabel(
          onTap: () {},
          iconPath: Assets.svg.profileAbout,
          label: 'ui.about'.tr(),
        ),
        const BeamDivider(),
        _IconLabel(
          onTap: () {},
          iconPath: Assets.svg.profileLogout,
          label: 'ui.signOut'.tr(),
        ),
        const BeamDivider(),
        _IconLabel(
          onTap: () {},
          iconPath: Assets.svg.profileDelete,
          label: 'ui.deleteAccount'.tr(),
        ),
      ],
    );
  }
}

class _IconLabel extends StatelessWidget {
  final String iconPath;
  final String label;
  final void Function()? onTap;

  // TODO(nausharipov): Auto-determine.
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
