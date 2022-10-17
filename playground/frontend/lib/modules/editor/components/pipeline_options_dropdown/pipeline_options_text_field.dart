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
import 'package:playground/constants/sizes.dart';
import 'package:playground_components/playground_components.dart';

class PipelineOptionsTextField extends StatelessWidget {
  final TextEditingController controller;
  final int lines;

  const PipelineOptionsTextField({
    Key? key,
    required this.controller,
    this.lines = 1,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final themeData = Theme.of(context);
    final ext = themeData.extension<BeamThemeExtension>()!;

    return Container(
      margin: const EdgeInsets.only(
        top: kMdSpacing,
      ),
      decoration: BoxDecoration(
        color: Theme.of(context).backgroundColor,
        borderRadius: BorderRadius.circular(kMdBorderRadius),
      ),
      child: ClipRRect(
        borderRadius: BorderRadius.circular(kMdBorderRadius),
        child: TextFormField(
          minLines: lines,
          maxLines: lines,
          controller: controller,
          decoration: InputDecoration(
            contentPadding: const EdgeInsets.all(kMdSpacing),
            border: _getInputBorder(ext.borderColor),
            focusedBorder: _getInputBorder(themeData.primaryColor),
          ),
        ),
      ),
    );
  }

  OutlineInputBorder _getInputBorder(Color color) {
    return OutlineInputBorder(
      borderSide: BorderSide(color: color),
      borderRadius: BorderRadius.circular(kMdBorderRadius),
    );
  }
}
