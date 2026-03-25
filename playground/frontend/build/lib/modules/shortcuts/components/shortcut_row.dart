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

class ShortcutRow extends StatelessWidget {
  final BeamShortcut shortcut;

  const ShortcutRow({Key? key, required this.shortcut}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final primaryColor = Theme.of(context).primaryColor;
    // wrap with row to shrink container to child size
    return Row(
      children: [
        Flexible(
          child: Container(
            decoration: BoxDecoration(
              border: Border.all(color: primaryColor),
              borderRadius: BorderRadius.circular(kSmBorderRadius),
            ),
            padding: const EdgeInsets.all(kMdSpacing),
            child: Text(
              shortcut.title,
              style: TextStyle(color: primaryColor),
            ),
          ),
        ),
      ],
    );
  }
}
