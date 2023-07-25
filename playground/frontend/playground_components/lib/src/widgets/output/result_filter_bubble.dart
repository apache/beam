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

import '../../enums/result_filter.dart';
import '../bubble.dart';

class ResultFilterBubble extends StatelessWidget {
  final ResultFilterEnum groupValue;
  final ValueChanged<ResultFilterEnum> onChanged;
  final String title;
  final ResultFilterEnum value;

  const ResultFilterBubble({
    super.key,
    required this.groupValue,
    required this.onChanged,
    required this.title,
    required this.value,
  });

  @override
  Widget build(BuildContext context) {
    final isSelected = value == groupValue;

    return BubbleWidget(
      isSelected: isSelected,
      onTap: () {
        if (!isSelected) {
          onChanged(value);
        }
      },
      title: title,
    );
  }
}
