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
import 'package:playground_components/playground_components.dart';

import '../../../constants/sizes.dart';
import '../../../models/unit_content.dart';
import '../state.dart';
import 'complete_unit_button.dart';
import 'hints.dart';
import 'markdown/tob_markdown.dart';

class ContentWidget extends StatelessWidget {
  final TourNotifier tourNotifier;

  const ContentWidget(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);

    return Container(
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
              if (currentUnitContent == null)
                Expanded(child: Container())
              else if (currentUnitContent.hints != null)
                Expanded(
                  child: _ChallengeContent(
                    unitContent: currentUnitContent,
                  ),
                )
              else
                Expanded(
                  child: TobMarkdown(
                    padding: const EdgeInsets.symmetric(
                      horizontal: BeamSizes.size12,
                      vertical: BeamSizes.size8,
                    ),
                    data: currentUnitContent.description,
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

class _ChallengeContent extends StatelessWidget {
  final UnitContentModel unitContent;

  const _ChallengeContent({
    required this.unitContent,
  });

  @override
  Widget build(BuildContext context) {
    final hints = unitContent.hints;

    return Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        if (hints != null)
          Padding(
            padding: const EdgeInsets.only(
              top: BeamSizes.size10,
              left: BeamSizes.size10,
              right: BeamSizes.size10,
            ),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                HintsWidget(
                  hints: hints,
                ),
                // TODO(nausharipov): solution button
                Container(),
              ],
            ),
          ),
        TobMarkdown(
          padding: const EdgeInsets.symmetric(
            horizontal: BeamSizes.size12,
            vertical: BeamSizes.size8,
          ),
          data: unitContent.description,
        ),
      ],
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
