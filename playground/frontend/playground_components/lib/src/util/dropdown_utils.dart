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

import '../widgets/dropdown_button/dropdown_button.dart';

const _bottomToDropdown = 10.0;

/// Returns the screen offset at which to show the dropdown.
///
/// [key] points to the button that triggers the dropdown.
/// [widgetWidth] is important when aligning to the right.
Offset findDropdownOffset({
  required GlobalKey key,
  DropdownAlignment alignment = DropdownAlignment.left,
  double widgetWidth = 0,
}) {
  final box = key.currentContext?.findRenderObject() as RenderBox?;

  if (box == null) {
    throw Exception('Cannot find render object for $key');
  }

  final buttonOffset = box.localToGlobal(Offset.zero);
  final top = buttonOffset.dy + box.size.height + _bottomToDropdown;

  switch (alignment) {
    case DropdownAlignment.left:
      return Offset(buttonOffset.dx, top);
    case DropdownAlignment.right:
      return Offset(buttonOffset.dx + box.size.width - widgetWidth, top);
  }
}
