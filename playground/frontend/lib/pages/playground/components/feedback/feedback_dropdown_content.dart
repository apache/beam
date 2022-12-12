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
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/fonts.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/pages/playground/components/feedback/feedback_dropdown_icon_button.dart';
import 'package:playground_components/playground_components.dart';

const double kTextFieldWidth = 365.0;
const double kTextFieldHeight = 68.0;
const String kFeedbackTitleText = 'Feedback';
const String kCancelButtonTitle = 'Cancel';
const String kSendFeedbackButtonTitle = 'Send feedback';
const String kFeedbackContentText = 'Have feedback? We\'d love to hear it,'
    ' but please don\'t share sensitive information.'
    '\nHave questions? Try help or support.';

class FeedbackDropdownContent extends StatelessWidget {
  final void Function() close;
  final TextEditingController textController;

  const FeedbackDropdownContent({
    Key? key,
    required this.close,
    required this.textController,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final borderColor = Theme.of(context).extension<BeamThemeExtension>()!.borderColor;

    final OutlineInputBorder border = OutlineInputBorder(
      borderSide: BorderSide(color: borderColor),
      borderRadius: BorderRadius.circular(kMdBorderRadius),
    );

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: kXlSpacing),
      child: Column(
        children: [
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: kXlSpacing),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Padding(
                      padding: const EdgeInsets.only(bottom: kXlSpacing),
                      child: Text(
                        kFeedbackTitleText,
                        style: getTitleFontStyle(
                          textStyle: const TextStyle(
                            fontSize: kFeedbackTitleFontSize,
                            fontWeight: kBoldWeight,
                          ),
                        ),
                      ),
                    ),
                    MouseRegion(
                      cursor: SystemMouseCursors.click,
                      child: GestureDetector(
                        onTap: () => close(),
                        child: const Icon(Icons.close),
                      ),
                    ),
                  ],
                ),
                Text(
                  kFeedbackContentText,
                  style: getTitleFontStyle(
                    textStyle: const TextStyle(
                      fontSize: kFeedbackContentFontSize,
                      fontWeight: kNormalWeight,
                    ),
                  ),
                ),
                Container(
                  margin: const EdgeInsets.only(
                    top: kMdSpacing,
                    bottom: kXlSpacing,
                  ),
                  width: kTextFieldWidth,
                  height: kTextFieldHeight,
                  decoration: BoxDecoration(
                    color: Theme.of(context).backgroundColor,
                    borderRadius: BorderRadius.circular(kMdBorderRadius),
                  ),
                  child: ClipRRect(
                    borderRadius: BorderRadius.circular(kMdBorderRadius),
                    child: TextFormField(
                      controller: textController,
                      decoration: InputDecoration(
                        focusedBorder: border,
                        enabledBorder: border,
                        contentPadding: const EdgeInsets.all(kMdSpacing),
                      ),
                      cursorColor: borderColor,
                      cursorWidth: kCursorSize,
                      onFieldSubmitted: (String filterText) {},
                      maxLines: 3,
                    ),
                  ),
                ),
              ],
            ),
          ),
          const BeamDivider(),
          Padding(
            padding: const EdgeInsets.only(
              top: kXlSpacing,
              left: kXlSpacing,
              right: kXlSpacing,
            ),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.end,
              crossAxisAlignment: CrossAxisAlignment.center,
              children: [
                Container(
                  height: kContainerHeight,
                  decoration: BoxDecoration(
                    color: Theme.of(context).backgroundColor,
                    borderRadius: BorderRadius.circular(kSmBorderRadius),
                    border: Border.all(
                      color: borderColor,
                    ),
                  ),
                  child: TextButton(
                    onPressed: () {
                      close();
                      textController.clear();
                    },
                    child: const Text(kCancelButtonTitle),
                  ),
                ),
                Container(
                  margin: const EdgeInsets.only(left: kLgSpacing),
                  height: kContainerHeight,
                  decoration: BoxDecoration(
                    color: Theme.of(context).primaryColor,
                    borderRadius: BorderRadius.circular(kSmBorderRadius),
                  ),
                  child: ElevatedButton(
                    onPressed: () {
                      if (textController.text.isNotEmpty) {
                        AnalyticsService.get(context).trackClickSendFeedback(
                          textController.text,
                        );
                      }
                      close();
                      textController.clear();
                    },
                    child: const Text(kSendFeedbackButtonTitle),
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}
