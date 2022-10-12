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

import '../../../constants/colors.dart';
import '../../../constants/sizes.dart';

class SignInOverlayContent extends StatelessWidget {
  const SignInOverlayContent();

  @override
  Widget build(BuildContext context) {
    return _Body(
      child: Column(
        children: [
          Text(
            'ui.signIn',
            style: Theme.of(context).textTheme.titleLarge,
          ).tr(),
          const SizedBox(height: TobSizes.size10),
          const Text(
            'dialogs.signInIf',
            textAlign: TextAlign.center,
          ).tr(),
          const _Divider(),
          // TODO(nausharipov): check branded buttons in firebase_auth
          ElevatedButton(
            onPressed: () {},
            child: const Text('ui.continueGitHub').tr(),
          ),
          const SizedBox(height: TobSizes.size16),
          ElevatedButton(
            onPressed: () {},
            child: const Text('ui.continueGoogle').tr(),
          ),
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
      elevation: TobSizes.size10,
      borderRadius: BorderRadius.circular(10),
      child: Container(
        width: TobSizes.authOverlayWidth,
        padding: const EdgeInsets.all(TobSizes.size24),
        child: child,
      ),
    );
  }
}

class _Divider extends StatelessWidget {
  const _Divider();

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.symmetric(vertical: 20),
      width: TobSizes.size32,
      height: TobSizes.size1,
      color: TobColors.grey3,
    );
  }
}
