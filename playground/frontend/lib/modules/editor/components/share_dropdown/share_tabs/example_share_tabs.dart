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

import '../../../../../pages/embedded_playground/path.dart';
import '../../../../../pages/standalone_playground/path.dart';
import '../../../../../utils/share_code_utils.dart';
import '../link_text_field.dart';
import '../share_tab_body.dart';
import 'share_format_enum.dart';

/// The content of the sharing dropdown at the point when the
/// shareable link can be generated from [descriptor].
// TODO(alexeyinkin): Refactor code sharing, https://github.com/apache/beam/issues/24637
class ExampleShareTabs extends StatelessWidget {
  final ExampleLoadingDescriptor descriptor;
  final EventSnippetContext eventSnippetContext;
  final Sdk sdk;
  final TabController tabController;

  const ExampleShareTabs({
    required this.descriptor,
    required this.eventSnippetContext,
    required this.sdk,
    required this.tabController,
  });

  @override
  Widget build(BuildContext context) {
    final appLocale = AppLocalizations.of(context)!;

    final standaloneUri = StandalonePlaygroundSinglePath(
      descriptor: descriptor,
    ).getUriAtBase(Uri.base);

    final embeddedUri = EmbeddedPlaygroundSinglePath(
      descriptor: descriptor,
      isEditable: true,
    ).getUriAtBase(Uri.base);

    return TabBarView(
      controller: tabController,
      physics: const NeverScrollableScrollPhysics(),
      children: [
        ShareTabBody(
          children: [
            Text(appLocale.linkReady),
            LinkTextField(
              eventSnippetContext: eventSnippetContext,
              shareFormat: ShareFormat.linkStandalone,
              text: standaloneUri.toString(),
            ),
          ],
        ),
        ShareTabBody(
          children: [
            Text(appLocale.iframeCodeReady),
            LinkTextField(
              eventSnippetContext: eventSnippetContext,
              shareFormat: ShareFormat.iframeEmbedded,
              text: ShareCodeUtils.iframe(
                src: embeddedUri,
              ),
            ),
          ],
        ),
      ],
    );
  }
}
