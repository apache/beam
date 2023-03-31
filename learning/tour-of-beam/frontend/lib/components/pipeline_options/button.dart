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
import 'package:playground_components/playground_components.dart';

import 'dropdown_body.dart';

class PipelineOptionsButton extends StatefulWidget {
  final PlaygroundController controller;

  const PipelineOptionsButton({required this.controller});

  @override
  PipelineOptionsButtonState createState() => PipelineOptionsButtonState();
}

class PipelineOptionsButtonState extends State<PipelineOptionsButton> {
  static const _dropdownWidth = 300.0;

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: widget.controller,
      builder: (BuildContext context, child) {
        final pipelineOptions =
            widget.controller.selectedExample?.pipelineOptions;

        if (pipelineOptions == null || pipelineOptions.isEmpty) {
          return const SizedBox.shrink();
        }

        final pipelineOptionsCount =
            _getPipelineOptionsCount(pipelineOptions).toString();
        return AppDropdownButton(
          buttonPadding: const EdgeInsets.all(BeamSpacing.small),
          buttonText: Text(
            'pages.tour.pipelineOptionsButtonTitle'
                .tr(namedArgs: {'count': pipelineOptionsCount}),
            style: Theme.of(context).textTheme.titleMedium,
          ),
          createDropdown: (close) {
            return PipelineOptionsDropdownBody(
              pipelineOptions: pipelineOptions,
              close: close,
            );
          },
          width: _dropdownWidth,
        );
      },
    );
  }

  int _getPipelineOptionsCount(String pipelineOptions) {
    return parsePipelineOptions(pipelineOptions)?.entries.length ?? 0;
  }
}
