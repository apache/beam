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
import 'package:playground/config/theme.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/sizes.dart';

const kSendFeedbackText = 'Send Feedback';
const kPrivacyPolicyText = 'Privacy Policy';
const kCopyright = '© The Apache Software Foundation';

class PlaygroundPageFooter extends StatelessWidget {
  const PlaygroundPageFooter({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      color: ThemeColors.of(context).secondaryBackground,
      width: double.infinity,
      child: Padding(
        padding: const EdgeInsets.symmetric(
          vertical: kMdSpacing,
          horizontal: kLgSpacing,
        ),
        child: Wrap(
          spacing: kLgSpacing,
          crossAxisAlignment: WrapCrossAlignment.center,
          children: [
            TextButton(
              style: TextButton.styleFrom(
                textStyle: const TextStyle(fontWeight: kNormalWeight),
                primary: Theme.of(context).primaryColor,
              ),
              // ignore: avoid_print
              onPressed: () => print(kSendFeedbackText),
              child: const Text(kSendFeedbackText),
            ),
            TextButton(
              style: TextButton.styleFrom(
                textStyle: const TextStyle(fontWeight: kNormalWeight),
              ),
              // ignore: avoid_print
              onPressed: () => print(kPrivacyPolicyText),
              child: const Text(kPrivacyPolicyText),
            ),
            Text(
              kCopyright,
              style: TextStyle(color: ThemeColors.of(context).grey1Color),
            ),
          ],
        ),
      ),
    );
  }
}
