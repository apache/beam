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
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import '../../../../pages/standalone_playground/notifiers/example_selector_state.dart';
import '../examples_components.dart';

/// The list of expandable example categories shown in the example dropdown.
class ExampleList extends StatefulWidget {
  final VoidCallback onSelected;
  final ExampleBase? selectedExample;

  const ExampleList({
    required this.onSelected,
    required this.selectedExample,
  });

  @override
  State<ExampleList> createState() => _ExampleListState();
}

class _ExampleListState extends State<ExampleList> {
  final _scrollController = ScrollController();

  @override
  Widget build(BuildContext context) {
    return Consumer<ExampleSelectorState>(
      builder: (context, state, child) => Expanded(
        child: Container(
          color: Theme.of(context).backgroundColor,
          child: Scrollbar(
            thumbVisibility: true,
            trackVisibility: true,
            controller: _scrollController,
            child: ListView.builder(
              itemCount: state.categories.length,
              itemBuilder: (context, index) => CategoryExpansionPanel(
                onSelected: widget.onSelected,
                selectedExample: widget.selectedExample,
                categoryName: state.categories[index].title,
                examples: state.categories[index].examples,
              ),
              controller: _scrollController,
              shrinkWrap: true,
            ),
          ),
        ),
      ),
    );
  }
}
