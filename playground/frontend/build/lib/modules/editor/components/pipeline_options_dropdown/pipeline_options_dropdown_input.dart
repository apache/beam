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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_option_label.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_text_field.dart';

const kPipelineOptionsInputLines = 8;

class PipelineOptionsDropdownInput extends StatelessWidget {
  final TextEditingController controller;

  const PipelineOptionsDropdownInput({
    Key? key,
    required this.controller,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        PipelineOptionLabel(text: appLocale.input),
        PipelineOptionsTextField(
          lines: kPipelineOptionsInputLines,
          controller: controller,
        ),
      ],
    );
  }
}
