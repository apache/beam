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
import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

Future<void> checkToggleBrightnessMode(WidgetTester wt) async {
  Brightness getBrightness() {
    return Theme.of(wt.element(find.toggleThemeButton())).brightness;
  }

  Future<void> toggleTheme() async {
    await wt.tap(find.toggleThemeButton());
    await wt.pumpAndSettle();
  }

  final startBrightness = getBrightness();
  final invertedBrightness =
      startBrightness == Brightness.light ? Brightness.dark : Brightness.light;

  await toggleTheme();
  expect(getBrightness(), invertedBrightness);
  await toggleTheme();
  expect(getBrightness(), startBrightness);
}
