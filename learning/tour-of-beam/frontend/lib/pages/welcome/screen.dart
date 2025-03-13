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

import 'package:app_state/app_state.dart';
import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/gestures.dart';
import 'package:flutter/material.dart';
import 'package:flutter_svg/svg.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../assets/assets.gen.dart';
import '../../auth/notifier.dart';
import '../../components/builders/content_tree.dart';
import '../../components/builders/sdks.dart';
import '../../components/login/content.dart';
import '../../components/scaffold.dart';
import '../../constants/sizes.dart';
import '../../models/module.dart';
import '../../state.dart';
import '../tour/page.dart';
import 'state.dart';

class WelcomeScreen extends StatelessWidget {
  final WelcomeNotifier welcomeNotifier;

  const WelcomeScreen(this.welcomeNotifier);

  @override
  Widget build(BuildContext context) {
    return TobScaffold(
      child: SingleChildScrollView(
        child: MediaQuery.of(context).size.width > ScreenBreakpoints.twoColumns
            ? _WideWelcome(welcomeNotifier)
            : _NarrowWelcome(welcomeNotifier),
      ),
    );
  }
}

class _WideWelcome extends StatelessWidget {
  final WelcomeNotifier welcomeNotifier;

  const _WideWelcome(this.welcomeNotifier);

  @override
  Widget build(BuildContext context) {
    return const IntrinsicHeight(
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Expanded(
            child: _SdkSelection(),
          ),
          Expanded(
            child: _TourSummary(),
          ),
        ],
      ),
    );
  }
}

class _NarrowWelcome extends StatelessWidget {
  final WelcomeNotifier welcomeNotifier;

  const _NarrowWelcome(this.welcomeNotifier);

  @override
  Widget build(BuildContext context) {
    return const Column(
      children: [
        _SdkSelection(),
        _TourSummary(),
      ],
    );
  }
}

class _SdkSelection extends StatelessWidget {
  const _SdkSelection();

  static const double _minimalHeight = 900;

  @override
  Widget build(BuildContext context) {
    final appNotifier = GetIt.instance.get<AppNotifier>();
    return Container(
      constraints: BoxConstraints(
        // TODO(nausharipov): look for a better way to constrain the height
        minHeight: MediaQuery.of(context).size.height -
            BeamSizes.appBarHeight -
            TobSizes.footerHeight,
      ),
      color: Theme.of(context).backgroundColor,
      child: Stack(
        children: [
          Positioned(
            bottom: 0,
            left: 0,
            right: 0,
            child: Theme.of(context).brightness == Brightness.dark
                ? Image.asset(Assets.png.laptopDark.path)
                : Image.asset(Assets.png.laptopLight.path),
          ),
          const SizedBox(height: _minimalHeight),
          Padding(
            padding: const EdgeInsets.fromLTRB(50, 60, 50, 20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const _IntroText(),
                const SizedBox(height: BeamSizes.size32),
                SdksBuilder(
                  builder: (context, sdks, child) {
                    if (sdks.isEmpty) {
                      return const Center(child: CircularProgressIndicator());
                    }

                    return AnimatedBuilder(
                      animation: appNotifier,
                      builder: (context, child) => _SdkButtons(
                        sdks: sdks,
                        groupValue: appNotifier.sdk,
                        onChanged: (v) => appNotifier.sdk = v,
                        onStartPressed: () async {
                          await _startTour(appNotifier.sdk);
                        },
                      ),
                    );
                  },
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Future<void> _startTour(Sdk? sdk) async {
    if (sdk == null) {
      return;
    }
    await GetIt.instance.get<PageStack>().push(TourPage());
  }
}

class _TourSummary extends StatelessWidget {
  const _TourSummary();

  @override
  Widget build(BuildContext context) {
    final appNotifier = GetIt.instance.get<AppNotifier>();
    return AnimatedBuilder(
      animation: appNotifier,
      builder: (context, child) {
        final sdk = appNotifier.sdk;

        return Padding(
          padding: const EdgeInsets.symmetric(
            vertical: BeamSizes.size20,
            horizontal: 27,
          ),
          child: ContentTreeBuilder(
            sdk: sdk,
            builder: (context, contentTree, child) {
              if (contentTree == null) {
                return const Center(child: CircularProgressIndicator());
              }

              return Column(
                children: contentTree.nodes
                    .map(
                      (module) => _Module(
                        module: module,
                        isLast: module == contentTree.nodes.last,
                      ),
                    )
                    .toList(growable: false),
              );
            },
          ),
        );
      },
    );
  }
}

class _IntroText extends StatelessWidget {
  const _IntroText();

  static const double _dividerMaxWidth = 150;

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'pages.welcome.title',
          style: Theme.of(context).textTheme.displayMedium,
        ).tr(),
        Container(
          margin: const EdgeInsets.symmetric(vertical: 32),
          height: BeamSizes.size2,
          color: BeamColors.grey2,
          constraints: const BoxConstraints(maxWidth: _dividerMaxWidth),
        ),
        const _IntroTextBody(),
      ],
    );
  }
}

class _IntroTextBody extends StatelessWidget {
  const _IntroTextBody();

  @override
  Widget build(BuildContext context) {
    final authNotifier = GetIt.instance.get<AuthNotifier>();
    return AnimatedBuilder(
      animation: authNotifier,
      builder: (context, child) => RichText(
        text: TextSpan(
          style: Theme.of(context).textTheme.bodyLarge,
          children: [
            TextSpan(
              text: 'pages.welcome.ifSaveProgress'.tr(),
            ),
            if (authNotifier.isAuthenticated)
              TextSpan(
                text: 'pages.welcome.signIn'.tr(),
              )
            else
              TextSpan(
                text: 'pages.welcome.signIn'.tr(),
                style: Theme.of(context)
                    .textTheme
                    .bodyLarge!
                    .copyWith(color: Theme.of(context).primaryColor),
                recognizer: TapGestureRecognizer()
                  ..onTap = () {
                    unawaited(_openLoginDialog(context));
                  },
              ),
            TextSpan(text: '\n\n${'pages.welcome.selectLanguage'.tr()}'),
          ],
        ),
      ),
    );
  }

  Future<void> _openLoginDialog(BuildContext context) async {
    await showDialog(
      context: context,
      builder: (context) => Dialog(
        child: LoginContent(
          onLoggedIn: () {
            Navigator.pop(context);
          },
        ),
      ),
    );
  }
}

class _SdkButtons extends StatelessWidget {
  final List<Sdk> sdks;
  final Sdk? groupValue;
  final ValueChanged<Sdk> onChanged;
  final VoidCallback onStartPressed;

  const _SdkButtons({
    required this.sdks,
    required this.groupValue,
    required this.onChanged,
    required this.onStartPressed,
  });

  @override
  Widget build(BuildContext context) {
    return Wrap(
      children: [
        Wrap(
          children: sdks
              .map(
                (sdk) => _SdkButton(
                  title: sdk.title,
                  value: sdk,
                  groupValue: groupValue,
                  onChanged: onChanged,
                ),
              )
              .toList(growable: false),
        ),
        ElevatedButton(
          onPressed: groupValue == null ? null : onStartPressed,
          child: const Text('pages.welcome.startTour').tr(),
        ),
      ],
    );
  }
}

class _SdkButton extends StatelessWidget {
  final String title;
  final Sdk value;
  final Sdk? groupValue;
  final ValueChanged<Sdk> onChanged;

  const _SdkButton({
    required this.title,
    required this.value,
    required this.groupValue,
    required this.onChanged,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(right: 15, bottom: 10),
      child: OutlinedButton(
        style: OutlinedButton.styleFrom(
          backgroundColor: Theme.of(context).backgroundColor,
          side: groupValue == value
              ? null
              : const BorderSide(color: BeamColors.grey1),
        ),
        onPressed: () {
          onChanged(value);
        },
        child: Text(title),
      ),
    );
  }
}

class _Module extends StatelessWidget {
  final ModuleModel module;
  final bool isLast;

  const _Module({
    required this.module,
    required this.isLast,
  });

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        _ModuleHeader(module: module),
        if (isLast) const _LastModuleBody() else const _ModuleBody(),
      ],
    );
  }
}

class _ModuleHeader extends StatelessWidget {
  final ModuleModel module;

  const _ModuleHeader({required this.module});

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisAlignment: MainAxisAlignment.spaceBetween,
      children: [
        Expanded(
          child: Row(
            children: [
              Padding(
                padding: const EdgeInsets.all(BeamSizes.size4),
                child: SvgPicture.asset(
                  Assets.svg.welcomeProgress0,
                  colorFilter: const ColorFilter.mode(
                    BeamColors.grey4,
                    BlendMode.srcIn,
                  ),
                ),
              ),
              const SizedBox(width: BeamSizes.size16),
              Expanded(
                child: Text(
                  module.title,
                  style: Theme.of(context).textTheme.titleLarge,
                ),
              ),
            ],
          ),
        ),
        Row(
          children: [
            Text(
              'complexity.${module.complexity.name}',
              style: Theme.of(context).textTheme.headlineSmall,
            ).tr(),
            const SizedBox(width: BeamSizes.size6),
            ComplexityWidget(complexity: module.complexity),
          ],
        ),
      ],
    );
  }
}

const EdgeInsets _moduleLeftMargin = EdgeInsets.only(left: 21);
const EdgeInsets _modulePadding = EdgeInsets.only(left: 39, top: 10);

class _ModuleBody extends StatelessWidget {
  const _ModuleBody();

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);

    return Container(
      margin: _moduleLeftMargin,
      decoration: BoxDecoration(
        border: Border(
          left: BorderSide(
            color: themeData.dividerColor,
          ),
        ),
      ),
      padding: _modulePadding,
      child: Column(
        children: [
          const SizedBox(height: BeamSizes.size16),
          Divider(
            color: themeData.dividerColor,
          ),
        ],
      ),
    );
  }
}

class _LastModuleBody extends StatelessWidget {
  const _LastModuleBody();

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: _moduleLeftMargin,
      padding: _modulePadding,
    );
  }
}
