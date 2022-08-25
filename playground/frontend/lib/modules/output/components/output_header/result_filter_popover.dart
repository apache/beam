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
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/models/outputs_model.dart';
import 'package:playground/modules/output/components/output_header/result_filter_bubble.dart';

const kPopoverWidth = 240.0;
const kPopoverPadding = 50.0;

class ResultFilterPopover extends StatelessWidget {
  const ResultFilterPopover({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    return Padding(
      padding: const EdgeInsets.only(top: kPopoverPadding),
      child: SizedBox(
        width: kPopoverWidth,
        child: Card(
          child: Padding(
            padding: const EdgeInsets.all(kMdSpacing),
            child: Wrap(
              runSpacing: kMdSpacing,
              children: [
                Text(appLocale.displayAtThisTab),
                Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: kSmSpacing,
                    vertical: kSmSpacing,
                  ),
                  child: Row(
                    children: [
                      ResultFilterBubble(
                        type: OutputType.all,
                        name: appLocale.all,
                      ),
                      ResultFilterBubble(
                        type: OutputType.log,
                        name: appLocale.log,
                      ),
                      ResultFilterBubble(
                        type: OutputType.output,
                        name: appLocale.output,
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
