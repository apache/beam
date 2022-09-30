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
import 'package:playground/modules/editor/components/share_dropdown/link_text_field.dart';
import 'package:playground/modules/editor/components/share_dropdown/share_tab_body.dart';
import 'package:playground/utils/share_code_utils.dart';

class ExampleShareTabs extends StatelessWidget {
  final String examplePath;
  final TabController tabController;

  const ExampleShareTabs({
    super.key,
    required this.examplePath,
    required this.tabController,
  });

  @override
  Widget build(BuildContext context) {
    final appLocale = AppLocalizations.of(context)!;

    return TabBarView(
      controller: tabController,
      physics: const NeverScrollableScrollPhysics(),
      children: [
        ShareTabBody(
          children: [
            Text(appLocale.linkReady),
            LinkTextField(
              text: ShareCodeUtils.examplePathToPlaygroundUrl(
                examplePath: examplePath,
                view: PlaygroundView.standalone,
              ).toString(),
            ),
          ],
        ),
        ShareTabBody(
          children: [
            Text(appLocale.iframeCodeReady),
            LinkTextField(
              text: ShareCodeUtils.examplePathToIframeCode(
                examplePath: examplePath,
              ),
            ),
          ],
        ),
      ],
    );
  }
}
