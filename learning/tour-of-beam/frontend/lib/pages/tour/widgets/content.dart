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
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../../auth/notifier.dart';
import '../../../cache/user_progress.dart';
import '../../../constants/sizes.dart';
import '../state.dart';
import 'unit_content.dart';

class ContentWidget extends StatelessWidget {
  final TourNotifier notifier;

  const ContentWidget(this.notifier);

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
        animation: notifier,
        builder: (context, child) {
          final currentUnitContent = notifier.currentUnitContent;

          return Column(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Expanded(
                child: currentUnitContent == null
                    ? Container()
                    : UnitContentWidget(unitContent: currentUnitContent),
              ),
              _ContentFooter(notifier),
            ],
          );
        },
      ),
    );
  }
}

class _ContentFooter extends StatelessWidget {
  final TourNotifier notifier;
  const _ContentFooter(this.notifier);

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
          _CompleteUnitButton(notifier),
        ],
      ),
    );
  }
}

class _CompleteUnitButton extends StatelessWidget {
  final TourNotifier notifier;
  const _CompleteUnitButton(this.notifier);

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);
    final userProgress = GetIt.instance.get<UserProgressCache>();
    final auth = GetIt.instance.get<AuthNotifier>();

    return AnimatedBuilder(
      animation: userProgress,
      builder: (context, child) {
        final isCompleted = userProgress
            .isUnitCompleted(notifier.contentTreeController.currentNode?.id);
        final isDisabled = !auth.isAuthenticated || isCompleted;

        return Flexible(
          child: OutlinedButton(
            style: OutlinedButton.styleFrom(
              foregroundColor: themeData.primaryColor,
              side: BorderSide(
                color: isDisabled
                    ? themeData.disabledColor
                    : themeData.primaryColor,
              ),
              shape: const RoundedRectangleBorder(
                borderRadius: BorderRadius.all(
                  Radius.circular(BeamSizes.size4),
                ),
              ),
            ),
            onPressed:
                isDisabled ? null : notifier.currentUnitController.completeUnit,
            child: const Text(
              'pages.tour.completeUnit',
              overflow: TextOverflow.ellipsis,
            ).tr(),
          ),
        );
      },
    );
  }
}
