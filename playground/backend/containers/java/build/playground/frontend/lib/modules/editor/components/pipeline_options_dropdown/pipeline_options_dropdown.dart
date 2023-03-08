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
import 'package:playground/components/dropdown_button/dropdown_button.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_body.dart';

const kDropdownWidth = 400.0;
const kDropdownHeight = 375.0;

class PipelineOptionsDropdown extends StatelessWidget {
  final String pipelineOptions;
  final void Function(String) setPipelineOptions;

  const PipelineOptionsDropdown({
    Key? key,
    required this.pipelineOptions,
    required this.setPipelineOptions,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    return AppDropdownButton(
      buttonText: Text(appLocale.pipelineOptions),
      height: kDropdownHeight,
      width: kDropdownWidth,
      createDropdown: (close) => PipelineOptionsDropdownBody(
        pipelineOptions: pipelineOptions,
        setPipelineOptions: setPipelineOptions,
        close: close,
      ),
    );
  }
}
