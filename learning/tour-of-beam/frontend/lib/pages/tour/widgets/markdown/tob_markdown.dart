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
import 'package:flutter_markdown_selectionarea/flutter_markdown_selectionarea.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

import 'code_builder.dart';

class TobMarkdown extends StatelessWidget {
  final String data;
  final EdgeInsets padding;
  final bool shrinkWrap;

  const TobMarkdown({
    required this.data,
    required this.padding,
    this.shrinkWrap = true,
  });

  static const _spaceCount = 4;

  @override
  Widget build(BuildContext context) {
    return SelectionArea(
      child: Markdown(
        data: data.tabsToSpaces(_spaceCount),
        builders: {
          'code': MarkdownCodeBuilder(),
        },
        onTapLink: (text, url, title) async {
          if (url != null) {
            await launchUrl(Uri.parse(url));
          }
        },
        padding: padding,
        shrinkWrap: shrinkWrap,
        styleSheet:
            Theme.of(context).extension<BeamThemeExtension>()!.markdownStyle,
      ),
    );
  }
}
