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

import 'package:flutter/gestures.dart';
import 'package:flutter/material.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover.dart';
import 'package:url_launcher/url_launcher.dart';

const kBannerTopPadding = 60.0;
const kDescriptionLineHeight = 1.5;
const kBannerTitle = '';
const kBannerDescription1 = '';
const kHyperlinkText = '';
const kBannerDescription2 = '';
const kBannerUrl = '';

class BannerDescription extends StatelessWidget {
  const BannerDescription({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(top: kBannerTopPadding),
      child: SizedBox(
        width: kDescriptionWidth + kSmSpacing,
        child: Card(
          child: Padding(
            padding: const EdgeInsets.all(kLgSpacing),
            child: Wrap(
              children: [
                Column(
                  children: [
                    const Align(
                      alignment: Alignment.centerLeft,
                      child: Text(
                        kBannerTitle,
                        style: TextStyle(
                          fontSize: kTitleFontSize,
                          fontWeight: kBoldWeight,
                        ),
                      ),
                    ),
                    RichText(
                      text: TextSpan(
                        children: [
                          const TextSpan(
                            text: kBannerDescription1,
                            style: TextStyle(
                              height: kDescriptionLineHeight,
                            ),
                          ),
                          TextSpan(
                              text: kBannerDescription2,
                              style: const TextStyle(color: Colors.blue),
                              mouseCursor: SystemMouseCursors.click,
                              recognizer: TapGestureRecognizer()
                                ..onTap = () async {
                                  launchUrl(Uri.parse(kBannerUrl));
                                }),
                          const TextSpan(
                            text: kHyperlinkText,
                            style: TextStyle(
                              height: kDescriptionLineHeight,
                            ),
                          ),
                        ],
                      ),
                    ),
                  ],
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
