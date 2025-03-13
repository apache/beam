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
import 'package:keyed_collection_widgets/keyed_collection_widgets.dart';
import 'package:playground_components/playground_components.dart';

import '../../components/scaffold.dart';
import '../../constants/sizes.dart';
import '../../enums/tour_view.dart';
import '../../shortcuts/shortcuts_manager.dart';
import 'state.dart';
import 'widgets/content_tree.dart';
import 'widgets/playground.dart';
import 'widgets/unit_content.dart';

class TourScreen extends StatelessWidget {
  final TourNotifier tourNotifier;
  static const dragHandleKey = Key('dragHandleKey');

  const TourScreen(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: tourNotifier,
      builder: (context, child) {
        return TobShortcutsManager(
          tourNotifier: tourNotifier,
          child: TobScaffold(
            playgroundController: tourNotifier.isPlaygroundShown
                ? tourNotifier.playgroundController
                : null,
            child:
                MediaQuery.of(context).size.width > ScreenBreakpoints.twoColumns
                    ? _WideTour(tourNotifier)
                    : _NarrowTour(tourNotifier),
          ),
        );
      },
    );
  }
}

class _WideTour extends StatelessWidget {
  final TourNotifier tourNotifier;

  const _WideTour(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        ContentTreeWidget(controller: tourNotifier.contentTreeController),
        Expanded(
          child: _UnitContentWidget(tourNotifier: tourNotifier),
        ),
      ],
    );
  }
}

class _UnitContentWidget extends StatelessWidget {
  const _UnitContentWidget({required this.tourNotifier});

  final TourNotifier tourNotifier;

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: tourNotifier,
      builder: (context, widget) {
        return tourNotifier.isPlaygroundShown
            ? _UnitContentWithPlaygroundSplitView(tourNotifier)
            : UnitContentWidget(tourNotifier);
      },
    );
  }
}

class _UnitContentWithPlaygroundSplitView extends StatelessWidget {
  final TourNotifier tourNotifier;
  const _UnitContentWithPlaygroundSplitView(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    final isPlaygroundLoading = tourNotifier.currentUnitContent == null ||
        tourNotifier.isSnippetLoading;

    return SplitView(
      direction: Axis.horizontal,
      dragHandleKey: TourScreen.dragHandleKey,
      first: UnitContentWidget(tourNotifier),
      second: isPlaygroundLoading
          ? const Center(child: CircularProgressIndicator())
          : PlaygroundWidget(
              tourNotifier: tourNotifier,
            ),
    );
  }
}

class _NarrowTour extends StatelessWidget {
  final TourNotifier tourNotifier;

  const _NarrowTour(this.tourNotifier);

  @override
  Widget build(BuildContext context) {
    final borderSide = BorderSide(
      color: Theme.of(context).dividerColor,
    );

    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        ContentTreeWidget(controller: tourNotifier.contentTreeController),
        Expanded(
          child: DefaultKeyedTabController.fromKeys(
            keys: TourView.values,
            child: Column(
              children: [
                DecoratedBox(
                  decoration: BoxDecoration(
                    border: Border(
                      left: borderSide,
                      bottom: borderSide,
                    ),
                  ),
                  child: KeyedTabBar.withDefaultController<TourView>(
                    tabs: UnmodifiableTourViewMap(
                      content: Tab(
                        text: 'pages.tour.content'.tr(),
                      ),
                      playground: Tab(
                        text: 'pages.tour.playground'.tr(),
                      ),
                    ),
                  ),
                ),
                Expanded(
                  child: KeyedTabBarView.withDefaultController<TourView>(
                    children: UnmodifiableTourViewMap(
                      content: UnitContentWidget(
                        tourNotifier,
                      ),
                      playground: PlaygroundWidget(
                        tourNotifier: tourNotifier,
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }
}
