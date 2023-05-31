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

import '../../../../services/analytics/events/share_code_clicked.dart';
import 'share_dropdown_body.dart';

const _kShareDropdownHeight = 140.0;
const _kShareDropdownWidth = 460.0;
const _kButtonColorOpacity = 0.2;

class ShareButton extends StatelessWidget {
  final PlaygroundController playgroundController;

  const ShareButton({
    required this.playgroundController,
  });

  @override
  Widget build(BuildContext context) {
    final parentThemeData = Theme.of(context);
    final ext = parentThemeData.extension<BeamThemeExtension>()!;
    final appLocale = AppLocalizations.of(context)!;

    final themeData = parentThemeData.copyWith(
      backgroundColor: ext.secondaryBackgroundColor,
      extensions: {
        ext.copyWith(
          fieldBackgroundColor:
              parentThemeData.primaryColor.withOpacity(_kButtonColorOpacity),
        ),
      },
    );

    return Theme(
      data: themeData,
      child: AppDropdownButton(
        buttonText: Text(appLocale.shareMyCode),
        showArrow: false,
        leading: Icon(
          Icons.share_outlined,
          color: themeData.primaryColor,
        ),
        height: _kShareDropdownHeight,
        width: _kShareDropdownWidth,
        dropdownAlign: DropdownAlignment.right,
        createDropdown: (closeCallback) {
          PlaygroundComponents.analyticsService.sendUnawaited(
            ShareCodeClickedAnalyticsEvent(
              snippetContext: playgroundController.eventSnippetContext,
            ),
          );
          return ShareDropdownBody(
            onError: closeCallback,
            playgroundController: playgroundController,
          );
        },
      ),
    );
  }
}
