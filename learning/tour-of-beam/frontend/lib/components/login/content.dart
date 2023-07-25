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
import 'package:firebase_auth_platform_interface/firebase_auth_platform_interface.dart';
import 'package:flutter/material.dart';
import 'package:flutter_svg/svg.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../assets/assets.gen.dart';
import '../../auth/notifier.dart';
import '../../constants/sizes.dart';

class LoginContent extends StatelessWidget {
  final VoidCallback onLoggedIn;

  const LoginContent({
    required this.onLoggedIn,
  });

  @override
  Widget build(BuildContext context) {
    return OverlayBody(
      child: Container(
        width: TobSizes.authOverlayWidth,
        padding: const EdgeInsets.all(BeamSizes.size24),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Text(
              'ui.signIn',
              style: Theme.of(context).textTheme.titleLarge,
            ).tr(),
            const SizedBox(height: BeamSizes.size10),
            const Text(
              'dialogs.signInIf',
              textAlign: TextAlign.center,
            ).tr(),
            const _Divider(),
            _BrandedLoginButtons(
              onLoggedIn: onLoggedIn,
            ),
          ],
        ),
      ),
    );
  }
}

class _Divider extends StatelessWidget {
  const _Divider();

  @override
  Widget build(BuildContext context) {
    return Container(
      color: BeamColors.grey3,
      margin: const EdgeInsets.symmetric(vertical: 20),
      width: BeamSizes.size32,
      height: BeamSizes.size1,
    );
  }
}

class _BrandedLoginButtons extends StatelessWidget {
  final VoidCallback onLoggedIn;

  const _BrandedLoginButtons({
    required this.onLoggedIn,
  });

  Future<void> _logIn(AuthProvider authProvider) async {
    await GetIt.instance.get<AuthNotifier>().logIn(authProvider);
    onLoggedIn();
  }

  @override
  Widget build(BuildContext context) {
    final isLightTheme = Theme.of(context).brightness == Brightness.light;
    final textStyle =
        MaterialStatePropertyAll(Theme.of(context).textTheme.bodyMedium);
    const padding = MaterialStatePropertyAll(
      EdgeInsets.symmetric(
        vertical: BeamSizes.size20,
        horizontal: BeamSizes.size24,
      ),
    );
    const minimumSize = MaterialStatePropertyAll(Size(double.infinity, 0));

    final darkButtonStyle = ButtonStyle(
      backgroundColor: const MaterialStatePropertyAll(BeamColors.darkGrey),
      minimumSize: minimumSize,
      padding: padding,
      textStyle: textStyle,
    );
    final githubLightButtonStyle = ButtonStyle(
      backgroundColor: const MaterialStatePropertyAll(BeamColors.darkBlue),
      minimumSize: minimumSize,
      padding: padding,
      textStyle: textStyle,
    );
    final googleLightButtonStyle = ButtonStyle(
      backgroundColor: const MaterialStatePropertyAll(BeamColors.white),
      elevation: const MaterialStatePropertyAll(BeamSizes.size4),
      foregroundColor: const MaterialStatePropertyAll(BeamColors.black),
      minimumSize: minimumSize,
      overlayColor: MaterialStatePropertyAll(Theme.of(context).hoverColor),
      padding: padding,
      textStyle: textStyle,
    );

    return Column(
      children: [
        ElevatedButton.icon(
          onPressed: () {
            _logIn(GithubAuthProvider());
          },
          style: isLightTheme ? githubLightButtonStyle : darkButtonStyle,
          icon: SvgPicture.asset(Assets.svg.githubLogo),
          label: const Text('ui.continueGitHub').tr(),
        ),
        const SizedBox(height: BeamSizes.size16),
        ElevatedButton.icon(
          onPressed: () {
            _logIn(GoogleAuthProvider());
          },
          style: isLightTheme ? googleLightButtonStyle : darkButtonStyle,
          icon: SvgPicture.asset(Assets.svg.googleLogo),
          label: const Text('ui.continueGoogle').tr(),
        ),
      ],
    );
  }
}
