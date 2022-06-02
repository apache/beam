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
import 'package:playground/config/theme.dart';
import 'package:playground/constants/sizes.dart';

/// Replaces the Flutter's Divider which is buggy with HTML renderer,
/// see https://github.com/flutter/flutter/issues/46339
class HorizontalDivider extends StatelessWidget {
  final double? indent;

  const HorizontalDivider({
    super.key,
    this.indent,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      height: kDividerHeight,
      margin: EdgeInsets.fromLTRB(indent ?? 0, 0, indent ?? 0, 0),
      color: ThemeColors.of(context).divider,
    );
  }
}
