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
import 'package:flutter_markdown/flutter_markdown.dart';
import 'package:markdown/markdown.dart' as md;
import 'package:playground_components/playground_components.dart';

import '../../../models/unit_content.dart';

class UnitContentWidget extends StatelessWidget {
  final UnitContentModel unitContent;

  const UnitContentWidget({
    required this.unitContent,
  });

  @override
  Widget build(BuildContext context) {
    return Markdown(
      selectable: true,
      data: unitContent.description,
      builders: {
        'code': CodeBuilder(),
      },
      styleSheet:
          Theme.of(context).extension<BeamThemeExtension>()!.markdownStyle,
    );
  }
}

class CodeBuilder extends MarkdownElementBuilder {
  @override
  Widget? visitElementAfter(md.Element element, TextStyle? preferredStyle) {
    final String textContent = element.textContent;
    final bool isCodeBlock = textContent.contains('\n');
    if (isCodeBlock) {
      /// codeblockDecoration is applied
      return null;
    }
    return _InlineCode(text: textContent);
  }
}

class _InlineCode extends StatelessWidget {
  final String text;
  const _InlineCode({required this.text});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(
        horizontal: BeamSizes.size3,
        vertical: BeamSizes.size1,
      ),
      decoration: BoxDecoration(
        color: Theme.of(context)
            .extension<BeamThemeExtension>()!
            .codeBackgroundColor,
        borderRadius: BorderRadius.circular(BeamSizes.size3),
      ),
      child: Text(
        text,
        style: TextStyle(color: Theme.of(context).primaryColor),
      ),
    );
  }
}
