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

import 'package:collection/collection.dart';
import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';

import '../../constants/sizes.dart';
import 'pipeline_option_controller.dart';
import 'pipeline_option_label.dart';
import 'pipeline_options_row.dart';

const kSpace = SizedBox(width: BeamSpacing.medium);

class PipelineOptionsForm extends StatelessWidget {
  final List<PipelineOptionController> options;
  final void Function(int) onDelete;

  const PipelineOptionsForm({
    super.key,
    required this.options,
    required this.onDelete,
  });

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Row(
          children: [
            Expanded(
              child: PipelineOptionLabel(
                text: 'widgets.pipelineOptions.name'.tr(),
              ),
            ),
            kSpace,
            Expanded(
              child: PipelineOptionLabel(
                text: 'widgets.pipelineOptions.value'.tr(),
              ),
            ),
            const SizedBox(width: BeamIconSizes.large),
          ],
        ),
        ...options.mapIndexed(
          (index, controller) => PipelineOptionsRow(
            controller: controller,
            onDelete: () => onDelete(index),
          ),
        ),
      ],
    );
  }
}
