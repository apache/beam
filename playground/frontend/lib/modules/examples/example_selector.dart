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
import 'package:playground/components/dropdown_button/dropdown_button.dart';
import 'package:playground/modules/examples/components/examples_components.dart';
import 'package:playground/modules/examples/components/search_field/search_field.dart';
import 'package:playground/modules/examples/models/category_model.dart';
import 'package:playground/pages/playground/states/example_dropdown_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

const double kLgContainerHeight = 444.0;
const double kLgContainerWidth = 400.0;

class ExampleSelector extends StatefulWidget {
  final List<CategoryModel> categories;

  const ExampleSelector({Key? key, required this.categories}) : super(key: key);

  @override
  State<ExampleSelector> createState() => _ExampleSelectorState();
}

class _ExampleSelectorState extends State<ExampleSelector> {
  final TextEditingController textController = TextEditingController();
  final ScrollController scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
  }

  @override
  void dispose() {
    scrollController.dispose();
    textController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AppDropdownButton(
      width: kLgContainerWidth,
      height: kLgContainerHeight,
      buttonText: Consumer<PlaygroundState>(
        builder: (context, state, child) => Text(state.examplesTitle),
      ),
      createDropdown: (_) => ChangeNotifierProvider(
        create: (context) => ExampleDropdownState(),
        builder: (context, _) => Column(
          children: [
            SearchField(controller: textController),
            const TypeFilter(),
            ExampleList(
              controller: scrollController,
              items: widget.categories,
            ),
          ],
        ),
      ),
    );
  }
}
