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
import 'package:flutter/gestures.dart';
import 'package:flutter/material.dart';
import 'package:flutter_svg/svg.dart';
import 'package:playground_components/playground_components.dart';

import '../../components/filler_text.dart';
import '../../components/scaffold.dart';
import '../../constants/sizes.dart';
import '../../generated/assets.gen.dart';

class WelcomeScreen extends StatelessWidget {
  const WelcomeScreen();

  @override
  Widget build(BuildContext context) {
    return TobScaffold(
      child: SingleChildScrollView(
        child: MediaQuery.of(context).size.width > ScreenBreakpoints.twoColumns
            ? const _WideWelcome()
            : const _NarrowWelcome(),
      ),
    );
  }
}

class _WideWelcome extends StatelessWidget {
  const _WideWelcome();

  @override
  Widget build(BuildContext context) {
    return IntrinsicHeight(
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: const [
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
  const _NarrowWelcome();

  @override
  Widget build(BuildContext context) {
    return Column(
      children: const [
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
    return Container(
      constraints: BoxConstraints(
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
              children: const [
                _IntroText(),
                SizedBox(height: BeamSizes.size32),
                _Buttons(),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _TourSummary extends StatelessWidget {
  const _TourSummary();

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(
        vertical: BeamSizes.size20,
        horizontal: 27,
      ),
      child: Column(
        children: _modules
            .map(
              (module) => _Module(
                title: module,
                isLast: module == _modules.last,
              ),
            )
            .toList(growable: false),
      ),
    );
  }

  static const List<String> _modules = [
    'Core Transforms',
    'Common Transforms',
    'IO',
    'Windowing',
    'Triggers',
  ];
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
        RichText(
          text: TextSpan(
            style: Theme.of(context).textTheme.bodyLarge,
            children: [
              TextSpan(
                text: 'pages.welcome.ifSaveProgress'.tr(),
              ),
              TextSpan(
                text: 'pages.welcome.signIn'.tr(),
                style: Theme.of(context)
                    .textTheme
                    .bodyLarge!
                    .copyWith(color: Theme.of(context).primaryColor),
                recognizer: TapGestureRecognizer()
                  ..onTap = () {
                    // TODO(nausharipov): sign in
                  },
              ),
              TextSpan(text: '\n\n${'pages.welcome.selectLanguage'.tr()}'),
            ],
          ),
        ),
      ],
    );
  }
}

class _Buttons extends StatelessWidget {
  const _Buttons();

  void _onSdkChanged(String value) {
    // TODO(nausharipov): change sdk
  }

  @override
  Widget build(BuildContext context) {
    return Wrap(
      children: [
        Wrap(
          children: ['Java', 'Python', 'Go']
              .map(
                (e) => _SdkButton(
                  value: e,
                  groupValue: _sdk,
                  onChanged: _onSdkChanged,
                ),
              )
              .toList(growable: false),
        ),
        ElevatedButton(
          onPressed: () {
            // TODO(nausharipov): redirect
          },
          child: const Text('pages.welcome.startTour').tr(),
        ),
      ],
    );
  }

  static const String _sdk = 'Java';
}

class _SdkButton extends StatelessWidget {
  final String value;
  final String groupValue;
  final ValueChanged<String> onChanged;

  const _SdkButton({
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
        child: Text(value),
      ),
    );
  }
}

class _Module extends StatelessWidget {
  final String title;
  final bool isLast;

  const _Module({
    required this.title,
    required this.isLast,
  });

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        _ModuleHeader(title: title),
        if (isLast) const _LastModuleBody() else const _ModuleBody(),
      ],
    );
  }
}

class _ModuleHeader extends StatelessWidget {
  final String title;
  const _ModuleHeader({required this.title});

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
                  color: BeamColors.grey4,
                ),
              ),
              const SizedBox(width: BeamSizes.size16),
              Expanded(
                child: Text(
                  title,
                  style: Theme.of(context).textTheme.titleLarge,
                ),
              ),
            ],
          ),
        ),
        Row(
          children: [
            Text(
              'complexity.medium',
              style: Theme.of(context).textTheme.headlineSmall,
            ).tr(),
            const SizedBox(width: BeamSizes.size6),
            const ComplexityWidget(complexity: Complexity.medium),
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
          const FillerText(width: 20),
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
      child: const FillerText(width: 20),
    );
  }
}
