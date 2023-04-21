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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import '../../../../constants/font_weight.dart';
import '../../notifiers/feedback_state.dart';
import 'feedback_dropdown_icon_button.dart';

/// A status bar item for feedback.
class PlaygroundFeedback extends StatelessWidget {
  const PlaygroundFeedback({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundController>(
      builder: (context, playgroundController, child) => Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Text(
            AppLocalizations.of(context)!.enjoyingPlayground,
            style: const TextStyle(fontWeight: kBoldWeight),
          ),
          FeedbackDropdownIconButton(
            key: Key(FeedbackRating.positive.name),
            feedbackRating: FeedbackRating.positive,
            isSelected: _getFeedbackState(context, true).feedbackRating ==
                FeedbackRating.positive,
            onClick: () => _onRated(
              context,
              FeedbackRating.positive,
              playgroundController,
            ),
            playgroundController: playgroundController,
          ),
          FeedbackDropdownIconButton(
            key: Key(FeedbackRating.negative.name),
            feedbackRating: FeedbackRating.negative,
            isSelected: _getFeedbackState(context, true).feedbackRating ==
                FeedbackRating.negative,
            onClick: () => _onRated(
              context,
              FeedbackRating.negative,
              playgroundController,
            ),
            playgroundController: playgroundController,
          ),
        ],
      ),
    );
  }

  void _onRated(
    BuildContext context,
    FeedbackRating rating,
    PlaygroundController playgroundController,
  ) {
    _getFeedbackState(context, false).setEnjoying(rating);

    PlaygroundComponents.analyticsService.sendUnawaited(
      AppRatedAnalyticsEvent(
        snippetContext: playgroundController.eventSnippetContext,
        rating: rating,
      ),
    );
  }

  FeedbackState _getFeedbackState(BuildContext context, bool listen) {
    return Provider.of<FeedbackState>(context, listen: listen);
  }
}
