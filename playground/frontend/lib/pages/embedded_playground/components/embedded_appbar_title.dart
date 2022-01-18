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
import 'package:flutter/services.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground/components/toggle_theme_button/toggle_theme_icon_button.dart';
import 'package:playground/constants/assets.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/editor/components/run_button.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/components/editor_textarea_wrapper.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

class EmbeddedAppBarTitle extends StatelessWidget {
  const EmbeddedAppBarTitle({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundState>(
      builder: (context, state, child) => Wrap(
        crossAxisAlignment: WrapCrossAlignment.center,
        spacing: kXlSpacing,
        children: [
          RunButton(
            isRunning: state.isCodeRunning,
            runCode: () {
              final stopwatch = Stopwatch()..start();
              state.runCode(
                onFinish: () {
                  AnalyticsService.get(context).trackRunTimeEvent(
                    state.selectedExample?.path ??
                        '$kUnknownExamplePrefix, sdk ${state.sdk.displayName}',
                    stopwatch.elapsedMilliseconds,
                  );
                },
              );
              AnalyticsService.get(context).trackClickRunEvent(
                state.selectedExample,
              );
            },
          ),
          const ToggleThemeIconButton(),
          IconButton(
            iconSize: kIconSizeLg,
            splashRadius: kIconButtonSplashRadius,
            icon: SvgPicture.asset(kCopyIconAsset),
            onPressed: () {
              final source =
                  Provider.of<PlaygroundState>(context, listen: false)
                      .source;
              Clipboard.setData(ClipboardData(text: source));
            },
          ),
        ],
      ),
    );
  }
}
