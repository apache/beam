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
import 'package:provider/provider.dart';

import 'common_finders.dart';

extension WidgetTesterExtension on WidgetTester {
  Future<void> enterCodeFieldText(String text) async {
    final codeField = widget(find.codeField());
    (codeField as CodeField).controller.fullText = text;
    codeField.focusNode?.requestFocus();
  }

  CodeController findOneCodeController() {
    final codeField = find.codeField();
    expect(codeField, findsOneWidget);

    return widget<CodeField>(codeField).controller;
  }

  TabController findOutputTabController() {
    final outputTabs = find.byType(OutputTabs);
    expect(outputTabs, findsOneWidget);

    return widget<OutputTabs>(outputTabs).tabController;
  }

  String? findOutputText() {
    final selectableText = find.outputSelectableText();
    expect(selectableText, findsOneWidget);

    return widget<SelectableText>(selectableText).data;
  }

  PlaygroundController findPlaygroundController() {
    final context = element(find.codeField());
    return context.read<PlaygroundController>();
  }

  Future<void> runShortcut(LogicalKeySet shortcut) async {
    final list = shortcut.keys.toList();
    for (final key in list) {
      await sendKeyDownEvent(key);
    }
    for (final key in list.reversed) {
      await sendKeyUpEvent(key);
    }
  }

  Future<void> tapAndSettle(Finder finder) async {
    await tap(finder);
    await pumpAndSettle();
  }

  Future<int> pumpAndSettleNoException({
    Duration duration = const Duration(milliseconds: 100),
    EnginePhase phase = EnginePhase.sendSemanticsUpdate,
    Duration timeout = const Duration(minutes: 10),
  }) async {
    return TestAsyncUtils.guard<int>(() async {
      final DateTime endTime = binding.clock.fromNowBy(timeout);
      int count = 0;
      do {
        if (binding.clock.now().isAfter(endTime)) {
          return count;
        }
        await binding.pump(duration, phase);
        count += 1;
      } while (binding.hasScheduledFrame);
      return count;
    });
  }
}
