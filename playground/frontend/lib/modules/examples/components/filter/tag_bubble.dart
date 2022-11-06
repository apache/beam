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
import 'package:playground/pages/playground/states/example_selector_state.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

class TagBubble extends StatelessWidget {
  final String name;

  const TagBubble({
    Key? key,
    required this.name,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<ExampleSelectorState>(
      builder: (context, state, child) {
        final isSelected = state.selectedTags.contains(name);

        return BubbleWidget(
          isSelected: isSelected,
          title: name,
          onTap: () {
            if (isSelected) {
              state.removeSelectedTag(name);
            } else {
              state.addSelectedTag(name);
            }
            state.filterCategoriesWithExamples();
          },
        );
      },
    );
  }
}
