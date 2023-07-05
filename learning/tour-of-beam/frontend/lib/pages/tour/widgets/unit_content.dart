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
import '../../../enums/save_code_status.dart';
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
      return const Center(child: CircularProgressIndicator());
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
        style: Theme.of(context).textTheme.titleLarge,
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

  @override
  Widget build(BuildContext context) {
    final hints = unitContent.hints;

    return Padding(
      padding: const EdgeInsets.all(BeamSizes.size10),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisAlignment: MainAxisAlignment.end,
        children: [
          if (hints.isNotEmpty)
            HintsWidget(
              hints: hints,
            ),
          _SnippetTypeSwitcher(
            tourNotifier: tourNotifier,
            unitContent: unitContent,
          ),
        ],
      ),
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

  Future<void> _setSnippetByType(SnippetType snippetType) async {
    await tourNotifier.showSnippetByType(snippetType);
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: tourNotifier,
      builder: (context, child) {
        final groupValue = tourNotifier.snippetType;

        return Row(
          children: [
            if (tourNotifier.hasSolution)
              _SnippetTypeButton(
                groupValue: groupValue,
                title: 'pages.tour.solution'.tr(),
                value: SnippetType.solution,
                onChanged: () async {
                  final confirmed = await ConfirmDialog.show(
                    context: context,
                    confirmButtonText: 'pages.tour.showSolution'.tr(),
                    subtitle: 'pages.tour.solveYourself'.tr(),
                    title: 'pages.tour.solution'.tr(),
                  );
                  if (confirmed) {
                    await _setSnippetByType(SnippetType.solution);
                  }
                },
              ),
            if (tourNotifier.hasSolution || tourNotifier.isCodeSaved)
              _SnippetTypeButton(
                groupValue: groupValue,
                title: unitContent.isChallenge
                    ? 'pages.tour.assignment'.tr()
                    : 'pages.tour.example'.tr(),
                value: SnippetType.original,
                onChanged: () async {
                  await _setSnippetByType(SnippetType.original);
                },
              ),
            if (tourNotifier.isCodeSaved)
              _SnippetTypeButton(
                groupValue: groupValue,
                title: tourNotifier.saveCodeStatus == SaveCodeStatus.saving
                    ? 'pages.tour.saving'.tr()
                    : 'pages.tour.myCode'.tr(),
                value: SnippetType.saved,
                onChanged: () async {
                  await _setSnippetByType(SnippetType.saved);
                },
              ),
          ],
        );
      },
    );
  }
}

class _SnippetTypeButton extends StatelessWidget {
  final SnippetType groupValue;
  final VoidCallback onChanged;
  final String title;
  final SnippetType value;

  const _SnippetTypeButton({
    required this.groupValue,
    required this.onChanged,
    required this.value,
    required this.title,
  });

  @override
  Widget build(BuildContext context) {
    final isSelected = value == groupValue;
    final Color? bgColor;
    final Color? fgColor;
    final VoidCallback? onPressed;
    if (isSelected) {
      bgColor = Theme.of(context).splashColor;
      fgColor = Theme.of(context).colorScheme.onSurface;
      onPressed = null;
    } else {
      bgColor = null;
      fgColor = null;
      onPressed = onChanged;
    }

    return TextButton(
      style: ButtonStyle(
        backgroundColor: MaterialStateProperty.all(bgColor),
        foregroundColor: MaterialStateProperty.all(fgColor),
      ),
      onPressed: onPressed,
      child: Text(title),
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
      padding: const EdgeInsets.all(BeamSizes.size10),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          IconButton(
            color: Theme.of(context).primaryColor,
            icon: const Icon(Icons.arrow_back_ios_new_rounded),
            onPressed: tourNotifier.contentTreeController.hasPreviousUnit()
                ? tourNotifier.contentTreeController.openPreviousUnit
                : null,
            tooltip: 'pages.tour.previousUnit'.tr(),
          ),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: BeamSizes.size10),
            child: CompleteUnitButton(tourNotifier),
          ),
          IconButton(
            color: Theme.of(context).primaryColor,
            icon: const Icon(Icons.arrow_forward_ios_rounded),
            onPressed: tourNotifier.contentTreeController.hasNextUnit()
                ? tourNotifier.contentTreeController.openNextUnit
                : null,
            tooltip: 'pages.tour.nextUnit'.tr(),
          ),
        ],
      ),
    );
  }
}
