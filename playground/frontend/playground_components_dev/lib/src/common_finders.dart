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
import 'package:flutter_code_editor/flutter_code_editor.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/playground_components.dart';

import 'finder.dart';

extension CommonFindersExtension on CommonFinders {
  Finder codeField() {
    return byType(CodeField);
  }

  Finder graphTab() {
    // TODO(alexeyinkin): Use keys when output tabs get to use enum, https://github.com/apache/beam/issues/22663
    return widgetWithText(OutputTab, 'Graph');
  }

  Finder outputSelectableText() {
    return find.descendant(
      of: find.outputWidget(),
      matching: find.byType(SelectableText),
    );
  }

  Finder outputWidget() {
    return byType(OutputWidget);
  }

  Finder resetButton() {
    return find.byType(ResetButton);
  }

  Finder resultTab() {
    return find.byType(ResultTab);
  }

  Finder runOrCancelButton() {
    return byType(RunOrCancelButton);
  }

  Finder splitView() {
    return byType(SplitView);
  }

  Finder toggleThemeButton() {
    return byType(ToggleThemeButton).or(byType(ToggleThemeIconButton));
  }
}
