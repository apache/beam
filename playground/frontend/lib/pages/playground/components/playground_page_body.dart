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
import 'package:playground/modules/output/components/output.dart';
import 'package:playground/modules/output/models/output_placement.dart';
import 'package:playground/pages/playground/components/editor_textarea_wrapper.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:provider/provider.dart';

class PlaygroundPageBody extends StatelessWidget {
  const PlaygroundPageBody({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<OutputPlacementState>(builder: (context, state, child) {
      switch (state.placement) {
        case OutputPlacement.bottom:
          return Column(children: [
            codeTextArea,
            getHorizontalSeparator(context),
            output,
          ]);
        case OutputPlacement.left:
          return Row(children: [
            output,
            getVerticalSeparator(context),
            codeTextArea,
          ]);
        case OutputPlacement.right:
          return Row(children: [
            codeTextArea,
            getVerticalSeparator(context),
            output,
          ]);
      }
    });
  }

  Widget get codeTextArea => const Expanded(child: CodeTextAreaWrapper());

  Widget get output => const Expanded(child: Output());

  Widget getVerticalSeparator(BuildContext context) => Container(
        width: kMdSpacing,
        color: ThemeColors.of(context).greyColor,
      );

  Widget getHorizontalSeparator(BuildContext context) => Container(
        height: kMdSpacing,
        color: ThemeColors.of(context).greyColor,
      );
}
