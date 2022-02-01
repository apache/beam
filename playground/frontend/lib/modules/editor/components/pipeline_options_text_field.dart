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
import 'package:playground/config/theme.dart';
import 'package:playground/constants/sizes.dart';

class PipelineOptionsTextField extends StatefulWidget {
  final String pipelineOptions;
  final Function(String value) onChange;

  const PipelineOptionsTextField(
      {Key? key, required this.pipelineOptions, required this.onChange})
      : super(key: key);

  @override
  State<PipelineOptionsTextField> createState() =>
      _PipelineOptionsTextFieldState();
}

class _PipelineOptionsTextFieldState extends State<PipelineOptionsTextField> {
  TextEditingController? _controller;

  @override
  void didChangeDependencies() {
    _controller = TextEditingController(text: widget.pipelineOptions);
    _controller?.addListener(() => widget.onChange(_controller?.text ?? ''));
    super.didChangeDependencies();
  }

  @override
  void dispose() {
    super.dispose();
    _controller?.dispose();
  }

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    return TextField(
      decoration: InputDecoration(
        // it should be prefix props, but text inside prefix disappears without focus
        icon: Padding(
          padding:
              const EdgeInsets.fromLTRB(kLgSpacing, kLgSpacing, 0, kLgSpacing),
          child: Text(
            appLocale.pipelineOptions,
            style: TextStyle(
              fontSize: kLabelFontSize,
              color: ThemeColors.of(context).textColor,
            ),
          ),
        ),
        border: InputBorder.none,
        hintText: appLocale.pipelineOptionsPlaceholder,
        hintStyle: TextStyle(
          fontSize: kHintFontSize,
          color: ThemeColors.of(context).grey1Color,
        ),
      ),
      controller: _controller,
    );
  }
}
