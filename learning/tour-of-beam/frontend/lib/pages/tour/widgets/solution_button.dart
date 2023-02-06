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
import 'package:flutter_svg/svg.dart';
import 'package:playground_components/playground_components.dart';

import '../../../assets/assets.gen.dart';
import '../../../constants/sizes.dart';
import '../state.dart';

class SolutionButton extends StatelessWidget {
  final TourNotifier tourNotifier;

  const SolutionButton({
    required this.tourNotifier,
  });

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: tourNotifier,
      builder: (context, child) => TextButton.icon(
        style: ButtonStyle(
          backgroundColor: MaterialStateProperty.all(
            tourNotifier.isShowingSolution
                ? Theme.of(context).splashColor
                : null,
          ),
        ),
        onPressed: () {
          showDialog(
            context: context,
            builder: (context) => Dialog(
              backgroundColor: Colors.transparent,
              child: _Popup(tourNotifier: tourNotifier),
            ),
          );
        },
        icon: SvgPicture.asset(Assets.svg.solution),
        label: Text(
          tourNotifier.isShowingSolution
              ? 'pages.tour.assignment'
              : 'pages.tour.solution',
        ).tr(),
      ),
    );
  }
}

class _Popup extends StatelessWidget {
  final TourNotifier tourNotifier;

  const _Popup({
    required this.tourNotifier,
  });

  @override
  Widget build(BuildContext context) {
    return OverlayBody(
      child: Container(
        width: TobSizes.hintPopupWidth,
        padding: const EdgeInsets.all(BeamSizes.size16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisSize: MainAxisSize.min,
          children: [
            Text(
              'pages.tour.solveYourself',
              style: Theme.of(context).textTheme.headlineMedium,
            ).tr(),
            const SizedBox(height: BeamSizes.size8),
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                TextButton(
                  onPressed: () {
                    Navigator.pop(context);
                  },
                  child: const Text('ui.cancel').tr(),
                ),
                const SizedBox(width: BeamSizes.size8),
                TextButton(
                  onPressed: () {
                    tourNotifier.toggleShowingSolution();
                    Navigator.pop(context);
                  },
                  child: const Text('pages.tour.showSolution').tr(),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
