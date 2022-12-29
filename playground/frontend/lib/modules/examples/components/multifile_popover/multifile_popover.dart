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
import 'package:playground/components/link_button.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground_components/playground_components.dart';

const kMultifileWidth = 300.0;

class MultifilePopover extends StatelessWidget {
  final ExampleBase example;

  const MultifilePopover({Key? key, required this.example}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    final hasGithubLink = example.urlVcs?.isNotEmpty ?? false;
    final hasNotebookLink = example.urlNotebook?.isNotEmpty ?? false;
    return SizedBox(
      width: kMultifileWidth,
      child: Card(
        child: Padding(
          padding: const EdgeInsets.all(kLgSpacing),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                appLocale.multifile,
                style: const TextStyle(
                  fontSize: kTitleFontSize,
                  fontWeight: kBoldWeight,
                ),
              ),
              const SizedBox(height: kMdSpacing),
              Text(appLocale.multifileWarning),
              const SizedBox(height: kMdSpacing),
              if (hasGithubLink) LinkButton.github(example.urlVcs ?? ''),
              if (hasNotebookLink) LinkButton.colab(example.urlNotebook ?? ''),
            ],
          ),
        ),
      ),
    );
  }
}
