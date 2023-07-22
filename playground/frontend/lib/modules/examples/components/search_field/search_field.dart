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
import 'package:provider/provider.dart';

import '../../../../constants/sizes.dart';
import '../../../../pages/standalone_playground/notifiers/example_selector_state.dart';

const double kContainerWidth = 376.0;
const int kMinLines = 1;
const int kMaxLines = 1;

class SearchField extends StatelessWidget {
  final TextEditingController controller;

  const SearchField({Key? key, required this.controller}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final borderColor =
        Theme.of(context).extension<BeamThemeExtension>()!.borderColor;

    final OutlineInputBorder border = OutlineInputBorder(
      borderSide: BorderSide(color: borderColor),
      borderRadius: BorderRadius.circular(kMdBorderRadius),
    );

    return Consumer<ExampleSelectorState>(
      builder: (context, state, child) => Container(
        margin: const EdgeInsets.only(
          top: kLgSpacing,
          right: kLgSpacing,
          left: kLgSpacing,
        ),
        width: kContainerWidth,
        height: kContainerHeight,
        color: Theme.of(context).backgroundColor,
        child: ClipRRect(
          borderRadius: BorderRadius.circular(kMdBorderRadius),
          child: TextFormField(
            controller: controller,
            decoration: InputDecoration(
              suffixIcon: Padding(
                padding: const EdgeInsetsDirectional.only(
                  start: kZeroSpacing,
                  end: kZeroSpacing,
                ),
                child: Icon(
                  Icons.search,
                  color: borderColor,
                  size: kIconSizeMd,
                ),
              ),
              focusedBorder: border,
              enabledBorder: border,
              filled: false,
              isDense: true,
              hintText: AppLocalizations.of(context)!.search,
              contentPadding: const EdgeInsets.only(left: kLgSpacing),
            ),
            cursorColor: borderColor,
            cursorWidth: kCursorSize,
            textAlignVertical: TextAlignVertical.center,
            onFieldSubmitted: (String filterText) =>
                _onChange(state, filterText),
            onChanged: (String filterText) => _onChange(state, filterText),
            maxLines: kMinLines,
            minLines: kMaxLines,
          ),
        ),
      ),
    );
  }

  _onChange(ExampleSelectorState state, String searchText) {
    state.setSearchText(searchText);
    state.filterCategoriesWithExamples();
  }
}
