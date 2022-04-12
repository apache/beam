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
import 'package:playground/constants/assets.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/pages/playground/components/feedback/feedback_dropdown_icon_button.dart';
import 'package:playground/pages/playground/states/feedback_state.dart';
import 'package:provider/provider.dart';

class PlaygroundFeedback extends StatelessWidget {
  const PlaygroundFeedback({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    final isEnjoying = _getFeedbackState(context, true).isEnjoying;
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Text(
          AppLocalizations.of(context)!.enjoyingPlayground,
          style: const TextStyle(fontWeight: kBoldWeight),
        ),
        FeedbackDropdownIconButton(
          label: appLocale.enjoying,
          iconAsset: kThumbUpIconAsset,
          filledIconAsset: kThumbUpIconAssetFilled,
          onClick: _setEnjoying(context, true),
          isSelected: isEnjoying != null && isEnjoying,
        ),
        FeedbackDropdownIconButton(
          label: appLocale.notEnjoying,
          iconAsset: kThumbDownIconAsset,
          filledIconAsset: kThumbDownIconAssetFilled,
          onClick: _setEnjoying(context, false),
          isSelected: isEnjoying != null && !isEnjoying,
        ),
      ],
    );
  }

  _setEnjoying(BuildContext context, bool isEnjoying) {
    return () {
      _getFeedbackState(context, false).setEnjoying(isEnjoying);
      AnalyticsService.get(context)
          .trackClickEnjoyPlayground(isEnjoying);
    };
  }

  FeedbackState _getFeedbackState(BuildContext context, bool listen) {
    return Provider.of<FeedbackState>(context, listen: listen);
  }
}
