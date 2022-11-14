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
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../state.dart';
import 'footer.dart';
import 'login/login_button.dart';
import 'logo.dart';
import 'profile/avatar.dart';
import 'sdk_dropdown.dart';

class TobScaffold extends StatelessWidget {
  final Widget child;
  final bool showSdkSelector;

  const TobScaffold({
    super.key,
    required this.child,
    required this.showSdkSelector,
  });

  // TODO(nausharipov): get state
  static const _isAuthorized = true;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Logo(),
        actions: [
          if (showSdkSelector)
            const _ActionVerticalPadding(child: _SdkSelector()),
          const SizedBox(width: BeamSizes.size12),
          const _ActionVerticalPadding(child: ToggleThemeButton()),
          const SizedBox(width: BeamSizes.size6),
          const _ActionVerticalPadding(
            child: _isAuthorized ? Avatar() : LoginButton(),
          ),
          const SizedBox(width: BeamSizes.size16),
        ],
      ),
      body: Column(
        children: [
          Expanded(child: child),
          const Footer(),
        ],
      ),
    );
  }
}

class _ActionVerticalPadding extends StatelessWidget {
  final Widget child;

  const _ActionVerticalPadding({required this.child});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: BeamSizes.size10),
      child: child,
    );
  }
}

class _SdkSelector extends StatelessWidget {
  const _SdkSelector();

  @override
  Widget build(BuildContext context) {
    final notifier = GetIt.instance.get<AppNotifier>();
    return AnimatedBuilder(
      animation: notifier,
      builder: (context, child) => notifier.sdkId == null
          ? Container()
          : SdkDropdown(
              sdkId: notifier.sdkId!,
              onChanged: (sdkId) {
                notifier.sdkId = sdkId;
              },
            ),
    );
  }
}
