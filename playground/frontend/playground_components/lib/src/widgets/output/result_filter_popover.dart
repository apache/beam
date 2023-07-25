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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';

import '../../constants/sizes.dart';
import '../../controllers/playground_controller.dart';
import '../../enums/result_filter.dart';
import 'result_filter_bubble.dart';

const kPopoverWidth = 240.0;
const kPopoverPadding = 50.0;

class ResultFilterPopover extends StatelessWidget {
  final PlaygroundController playgroundController;

  const ResultFilterPopover({
    required this.playgroundController,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(top: kPopoverPadding),
      child: SizedBox(
        width: kPopoverWidth,
        child: Card(
          child: Padding(
            padding: const EdgeInsets.all(BeamSizes.size8),
            child: Wrap(
              runSpacing: BeamSizes.size8,
              children: [
                const Text('widgets.output.filterTitle').tr(),
                Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: BeamSizes.size4,
                    vertical: BeamSizes.size4,
                  ),
                  child: AnimatedBuilder(
                    animation: playgroundController.resultFilterController,
                    builder: (context, child) {
                      final groupValue = playgroundController
                          .resultFilterController.value;

                      return Row(
                        children: [
                          for (final value in ResultFilterEnum.values)
                            ResultFilterBubble(
                              groupValue: groupValue,
                              onChanged: playgroundController
                                  .resultFilterController.setValue,
                              title: 'widgets.output.filter.${value.name}'.tr(),
                              value: value,
                            ),
                        ],
                      );
                    },
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
