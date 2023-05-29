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
import 'package:flutter/services.dart';

import '../../constants/sizes.dart';
import 'pipeline_option_controller.dart';
import 'pipeline_options_form.dart';
import 'pipeline_options_text_field.dart';

class PipelineOptionsRow extends StatelessWidget {
  final void Function() onDelete;
  final PipelineOptionController controller;

  const PipelineOptionsRow({
    required this.controller,
    required this.onDelete,
  });

  @override
  Widget build(BuildContext context) {
    final inputFormatters =  [
            FilteringTextInputFormatter.deny(RegExp(r'\s')),
          ];
    return Row(
      children: [
        Expanded(
          child: SizedBox(
            height: BeamSizes.textFieldHeight,
            child: PipelineOptionsTextField(
              controller: controller.nameController,
              inputFormatters: inputFormatters,
            ),
          ),
        ),
        kSpace,
        Expanded(
          child: SizedBox(
            height: BeamSizes.textFieldHeight,
            child: PipelineOptionsTextField(
              controller: controller.valueController,
              inputFormatters: inputFormatters,
            ),
          ),
        ),
        SizedBox(
          width: BeamIconSizes.large,
          child: IconButton(
            iconSize: BeamIconSizes.medium,
            splashRadius: BeamIconSizes.largeSplashRadius,
            icon: Icon(
              Icons.delete_outlined,
              color: Theme.of(context).primaryColor,
            ),
            color: Theme.of(context).dividerColor,
            onPressed: onDelete,
          ),
        ),
      ],
    );
  }
}
