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
import 'package:flutter/material.dart';
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground/constants/colors.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_option_controller.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_option_label.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_text_field.dart';

const kSpace = SizedBox(width: kMdSpacing);
const kTextFieldHeight = 50.0;

class PipelineOptionsForm extends StatelessWidget {
  final List<PipelineOptionController> options;
  final void Function(int) onDelete;

  const PipelineOptionsForm({
    Key? key,
    required this.options,
    required this.onDelete,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    return Column(
      children: [
        Row(
          children: [
            Expanded(child: PipelineOptionLabel(text: appLocale.name)),
            kSpace,
            Expanded(child: PipelineOptionLabel(text: appLocale.value)),
            const SizedBox(width: kIconSizeLg),
          ],
        ),
        ...options.mapIndexed(
          (index, controller) => Row(
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
                  onPressed: () => onDelete(index),
                ),
              ),
            ],
          ),
        )
      ],
    );
  }
}
