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

import 'package:app_state/app_state.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../pages/tour/widgets/pipeline_options.dart';
import '../state.dart';
import 'footer.dart';
import 'login/button.dart';
import 'logo.dart';
import 'profile/avatar.dart';
import 'sdk_dropdown.dart';

class TobScaffold extends StatelessWidget {
  final Widget child;
  final PlaygroundController? playgroundController;

  const TobScaffold({
    required this.child,
    this.playgroundController,
  });

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        automaticallyImplyLeading: false,
        title: const _HomepageLinkLogo(),
        actions: [
          if (playgroundController != null)
            _PlaygroundControllerActions(
              playgroundController: playgroundController!,
            ),
          const SizedBox(width: BeamSizes.size12),
          const _ActionVerticalPadding(child: _SdkSelector()),
          const SizedBox(width: BeamSizes.size12),
          const _ActionVerticalPadding(child: ToggleThemeButton()),
          const SizedBox(width: BeamSizes.size6),
          const _ActionVerticalPadding(child: _Profile()),
          const SizedBox(width: BeamSizes.size16),
        ],
      ),
      body: Column(
        children: [
          Expanded(child: child),
          Footer(
            playgroundController: playgroundController,
          ),
        ],
      ),
    );
  }
}

class _HomepageLinkLogo extends StatelessWidget {
  const _HomepageLinkLogo();

  @override
  Widget build(BuildContext context) {
    return MouseRegion(
      cursor: SystemMouseCursors.click,
      child: GestureDetector(
        onTap: () {
          GetIt.instance.get<PageStack>().popUntilBottom();
        },
        child: const Logo(),
      ),
    );
  }
}

class _Profile extends StatelessWidget {
  const _Profile();

  @override
  Widget build(BuildContext context) {
    return StreamBuilder(
      stream: FirebaseAuth.instance.userChanges(),
      builder: (context, snapshot) {
        final user = snapshot.data;
        return user == null ? const LoginButton() : Avatar(user: user);
      },
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
    final appNotifier = GetIt.instance.get<AppNotifier>();
    return AnimatedBuilder(
      animation: appNotifier,
      builder: (context, child) {
        return SdkDropdown(
          value: appNotifier.sdk,
          onChanged: (value) {
            appNotifier.sdk = value;
          },
        );
      },
    );
  }
}

class _PlaygroundControllerActions extends StatelessWidget {
  final PlaygroundController playgroundController;

  const _PlaygroundControllerActions({
    required this.playgroundController,
  });

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: playgroundController,
      builder: (context, child) {
        final widgets = <Widget>[];
        widgets.add(
          _ActionVerticalPadding(
            child: TobPipelineOptionsDropdown(
              playgroundController: playgroundController,
            ),
          ),
        );
        return Row(
          children: widgets
              .map(
                (e) => Padding(
                  padding: const EdgeInsets.only(left: BeamSizes.size12),
                  child: e,
                ),
              )
              .toList(growable: false),
        );
      },
    );
  }
}
