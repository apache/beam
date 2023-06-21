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
import 'package:markdown/markdown.dart' as md;
import 'package:playground_components/playground_components.dart';

class MarkdownCodeBuilder extends MarkdownElementBuilder {
  @override
  Widget? visitElementAfter(md.Element element, TextStyle? preferredStyle) {
    final String textContent = element.textContent;
    final bool isCodeBlock = textContent.contains('\n');
    if (isCodeBlock) {
      return _CodeBlock(text: textContent);
    }
    return _InlineCode(text: textContent);
  }
}

class _CodeBlock extends StatelessWidget {
  final String text;
  const _CodeBlock({required this.text});

  @override
  Widget build(BuildContext context) {
    final scrollController = ScrollController();
    return Padding(
      padding: const EdgeInsets.all(BeamSizes.size4),
      child: Scrollbar(
        controller: scrollController,
        scrollbarOrientation: ScrollbarOrientation.bottom,
        thumbVisibility: true,
        child: SingleChildScrollView(
          controller: scrollController,
          padding: const EdgeInsets.all(BeamSizes.size10),
          scrollDirection: Axis.horizontal,
          child: Text(
            text,
            style: Theme.of(context)
                .extension<BeamThemeExtension>()!
                .markdownStyle
                .code,
          ),
        ),
      ),
    );
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
        style: Theme.of(context)
            .extension<BeamThemeExtension>()!
            .markdownStyle
            .p!
            .copyWith(
              color: Theme.of(context).primaryColor,
            ),
      ),
    );
  }
}
