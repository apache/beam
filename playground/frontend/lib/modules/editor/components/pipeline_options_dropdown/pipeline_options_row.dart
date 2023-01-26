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
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_option_controller.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_form.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_text_field.dart';

import '../../../../constants/colors.dart';
import '../../../../constants/sizes.dart';

const kTextFieldHeight = 50.0;

class PipelineOptionsRow extends StatelessWidget {
  final void Function() onDelete;
  final PipelineOptionController controller;

  const PipelineOptionsRow({
    required this.controller,
    required this.onDelete,
  });

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        Expanded(
          child: SizedBox(
            height: kTextFieldHeight,
            child: PipelineOptionsTextField(
              controller: controller.nameController,
            ),
          ),
        ),
        kSpace,
        Expanded(
          child: SizedBox(
            height: kTextFieldHeight,
            child: PipelineOptionsTextField(
              controller: controller.valueController,
            ),
          ),
        ),
        SizedBox(
          width: kIconSizeLg,
          child: IconButton(
            iconSize: kIconSizeMd,
            splashRadius: kIconButtonSplashRadius,
            icon: const Icon(
              Icons.delete_outlined,
              color: kLightPrimary,
            ),
            color: Theme.of(context).dividerColor,
            onPressed: () => onDelete(),
          ),
        ),
      ],
    );
  }
}
