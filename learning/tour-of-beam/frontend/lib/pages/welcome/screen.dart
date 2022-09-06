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

import '../../components/complexity.dart';
import '../../components/page_container.dart';
import '../../config/theme/colors_provider.dart';
import '../../constants/assets.dart';
import '../../constants/colors.dart';
import '../../constants/sizes.dart';

class WelcomeScreen extends StatelessWidget {
  const WelcomeScreen();

  @override
  Widget build(BuildContext context) {
    return PageContainer(
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
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: const [
        Expanded(
          child: _SdkSelection(),
        ),
        Expanded(
          child: _TourSummary(),
        ),
      ],
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

  @override
  Widget build(BuildContext context) {
    return Container(
      constraints: BoxConstraints(
        minHeight: MediaQuery.of(context).size.height -
            TobSizes.appBarHeight -
            TobSizes.footerHeight,
      ),
      color: ThemeColors.of(context).background,
      child: Stack(
        children: [
          Positioned(
            bottom: 0,
            left: 0,
            right: 0,
            // TODO(nausharipov): use flutter_gen after merging
            child: Theme.of(context).brightness == Brightness.dark
                ? Image.asset(TobAssets.laptopDark)
                : Image.asset(TobAssets.laptopLight),
          ),
          const SizedBox(height: 900),
          Padding(
            padding: const EdgeInsets.fromLTRB(50, 60, 50, 20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: const [
                _IntroText(),
                SizedBox(height: TobSizes.size32),
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
        vertical: TobSizes.size20,
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
            .toList(),
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
          height: 2,
          color: TobColors.grey2,
          constraints: const BoxConstraints(maxWidth: 150),
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
                    .copyWith(color: ThemeColors.of(context).primary),
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
    // TODO(nausharipov): select the language
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
              .toList(),
        ),
        ElevatedButton(
          onPressed: () {
            // TODO(nausharipov): redirect
          },
          child: const Text('pages.welcome.startLearning').tr(),
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
          backgroundColor: ThemeColors.of(context).background,
          side: groupValue == value
              ? null
              : const BorderSide(color: TobColors.grey1),
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
                padding: const EdgeInsets.all(TobSizes.size4),
                child: SvgPicture.asset(
                  TobAssets.welcomeProgress0,
                  color: ThemeColors.of(context).progressBackgroundColor,
                ),
              ),
              const SizedBox(width: TobSizes.size16),
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
            const SizedBox(width: TobSizes.size6),
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
    return Container(
      margin: _moduleLeftMargin,
      decoration: BoxDecoration(
        border: Border(
          left: BorderSide(
            color: ThemeColors.of(context).divider,
          ),
        ),
      ),
      padding: _modulePadding,
      child: Column(
        children: [
          const Text(
            'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam velit purus, tincidunt id velit vitae, mattis dictum velit. Nunc sit amet nunc at turpis eleifend commodo ac ut libero. Aenean rutrum rutrum nulla ut efficitur. Vestibulum pulvinar eros dictum lectus volutpat dignissim vitae quis nisi. Maecenas sem erat, elementum in euismod ut, interdum ac massa.',
          ),
          const SizedBox(height: TobSizes.size16),
          Divider(
            color: ThemeColors.of(context).divider,
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
      child: const Text(
        'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam velit purus, tincidunt id velit vitae, mattis dictum velit. Nunc sit amet nunc at turpis eleifend commodo ac ut libero. Aenean rutrum rutrum nulla ut efficitur. Vestibulum pulvinar eros dictum lectus volutpat dignissim vitae quis nisi. Maecenas sem erat, elementum in euismod ut, interdum ac massa.',
      ),
    );
  }
}
