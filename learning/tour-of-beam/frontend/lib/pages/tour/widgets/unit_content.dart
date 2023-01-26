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

import 'package:playground_components/playground_components.dart';

import '../../../constants/sizes.dart';
import '../../../enums/snippet_type.dart';
import '../../../models/unit_content.dart';
import '../state.dart';
import 'complete_unit_button.dart';
import 'hints.dart';
import 'markdown/tob_markdown.dart';

class UnitContentWidget extends StatelessWidget {
  final TourNotifier tourNotifier;

  const UnitContentWidget(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);

    return Container(
      // TODO(nausharipov): look for a better way to constrain the height
      height: MediaQuery.of(context).size.height -
          BeamSizes.appBarHeight -
          TobSizes.footerHeight,
      decoration: BoxDecoration(
        color: themeData.backgroundColor,
        border: Border(
          left: BorderSide(color: themeData.dividerColor),
        ),
      ),
      child: AnimatedBuilder(
        animation: tourNotifier,
        builder: (context, child) {
          final currentUnitContent = tourNotifier.currentUnitContent;

          return Column(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Expanded(
                child: _Content(
                  tourNotifier: tourNotifier,
                  unitContent: currentUnitContent,
                ),
              ),
              _ContentFooter(tourNotifier),
            ],
          );
        },
      ),
    );
  }
}

class _Content extends StatelessWidget {
  final TourNotifier tourNotifier;
  final UnitContentModel? unitContent;

  const _Content({
    required this.tourNotifier,
    required this.unitContent,
  });

  @override
  Widget build(BuildContext context) {
    final content = unitContent;
    if (content == null) {
      return Container();
    }

    return ListView(
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            _Title(title: content.title),
            _Buttons(
              unitContent: content,
              tourNotifier: tourNotifier,
            ),
          ],
        ),
        TobMarkdown(
          padding: const EdgeInsets.all(BeamSizes.size12),
          data: content.description,
        ),
      ],
    );
  }
}

class _Title extends StatelessWidget {
  final String title;

  const _Title({
    required this.title,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(
        top: BeamSizes.size12,
        left: BeamSizes.size12,
        right: BeamSizes.size12,
      ),
      child: Text(
        title,
        style: Theme.of(context).textTheme.headlineLarge,
        textAlign: TextAlign.start,
      ),
    );
  }
}

class _Buttons extends StatelessWidget {
  final TourNotifier tourNotifier;
  final UnitContentModel unitContent;

  const _Buttons({
    required this.tourNotifier,
    required this.unitContent,
  });

  static const _buttonPadding = EdgeInsets.only(
    top: BeamSizes.size10,
    right: BeamSizes.size10,
  );

  @override
  Widget build(BuildContext context) {
    final hints = unitContent.hints;

    return Row(
      mainAxisAlignment: MainAxisAlignment.end,
      children: [
        _SnippetTypeSwitcher(
          tourNotifier: tourNotifier,
          unitContent: unitContent,
        ),
        if (hints.isNotEmpty)
          Padding(
            padding: _buttonPadding,
            child: HintsWidget(
              hints: hints,
            ),
          ),
      ],
    );
  }
}

class _SnippetTypeSwitcher extends StatelessWidget {
  final TourNotifier tourNotifier;
  final UnitContentModel unitContent;

  const _SnippetTypeSwitcher({
    required this.tourNotifier,
    required this.unitContent,
  });

  @override
  Widget build(BuildContext context) {
    // TODO(nausharipov): styling
    return AnimatedBuilder(
      animation: tourNotifier,
      builder: (context, child) => Row(
        children: [
          if (tourNotifier.hasSolution)
            _SnippetTypeButton(
              buttonSnippetType: SnippetType.solution,
              tourNotifier: tourNotifier,
              label: 'pages.tour.solution'.tr(),
            ),
          if (tourNotifier.hasSavedSnippet) ...[
            _SnippetTypeButton(
              buttonSnippetType: SnippetType.original,
              tourNotifier: tourNotifier,
              label: unitContent.isChallenge
                  ? 'pages.tour.assignment'.tr()
                  : 'pages.tour.example'.tr(),
            ),
            _SnippetTypeButton(
              buttonSnippetType: SnippetType.saved,
              tourNotifier: tourNotifier,
              label: 'pages.tour.myCode'.tr(),
            ),
          ]
        ],
      ),
    );
  }
}

class _SnippetTypeButton extends StatelessWidget {
  final SnippetType buttonSnippetType;
  final String label;
  final TourNotifier tourNotifier;

  const _SnippetTypeButton({
    required this.buttonSnippetType,
    required this.tourNotifier,
    required this.label,
  });

  Future<void> _setSnippetByType() async {
    await tourNotifier.setSnippetByType(buttonSnippetType);
  }

  @override
  Widget build(BuildContext context) {
    return TextButton(
      onPressed: buttonSnippetType == tourNotifier.snippetType
          ? null
          : _setSnippetByType,
      child: Text(label),
    );
  }
}

class _ContentFooter extends StatelessWidget {
  final TourNotifier tourNotifier;
  const _ContentFooter(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);

    return Container(
      decoration: BoxDecoration(
        border: Border(
          top: BorderSide(color: themeData.dividerColor),
        ),
        color:
            themeData.extension<BeamThemeExtension>()?.secondaryBackgroundColor,
      ),
      width: double.infinity,
      padding: const EdgeInsets.all(BeamSizes.size20),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.end,
        children: [
          CompleteUnitButton(tourNotifier),
        ],
      ),
    );
  }
}
