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
import 'package:playground/constants/sizes.dart';
import 'package:playground/pages/playground/states/example_dropdown_state.dart';
import 'package:provider/provider.dart';

class CategoryBubble extends StatelessWidget {
  final String name;

  const CategoryBubble({Key? key, required this.name}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(right: kMdSpacing),
      child: Consumer<ExampleDropdownState>(
        builder: (context, state, child) => MouseRegion(
          cursor: SystemMouseCursors.click,
          child: GestureDetector(
            onTap: () {
              if (name != state.selectedCategory) {
                state.setSelectedCategory(name);
              }
            },
            child: Container(
              height: kContainerHeight,
              padding: const EdgeInsets.symmetric(horizontal: kXlSpacing),
              decoration: BoxDecoration(
                color: name == state.selectedCategory
                    ? ThemeColors.of(context).primary
                    : ThemeColors.of(context).lightGreyColor,
                borderRadius: BorderRadius.circular(kXlBorderRadius),
              ),
              child: Center(
                child: Text(
                  name,
                  style: TextStyle(
                    color: name == state.selectedCategory
                        ? ThemeColors.of(context).primaryBackgroundTextColor
                        : ThemeColors.of(context).lightGreyBackgroundTextColor,
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
