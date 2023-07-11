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

import 'package:app_state/app_state.dart';
import 'package:flutter/material.dart';
import 'package:flutter_code_editor/flutter_code_editor.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:get_it/get_it.dart';
import 'package:keyed_collection_widgets/keyed_collection_widgets.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import 'common_finders.dart';
import 'examples/example_descriptor.dart';
import 'expect.dart';

extension WidgetTesterExtension on WidgetTester {
  //workaround for https://github.com/flutter/flutter/issues/120060
  Future<void> enterCodeFieldText(String text) async {
    final codeField = widget(find.snippetCodeField());
    (codeField as CodeField).controller.fullText = text;
    codeField.focusNode?.requestFocus();
  }

  Brightness getBrightness() {
    final context = element(find.toggleThemeButton());
    return Theme.of(context).brightness;
  }

  Future<void> toggleTheme() async {
    await tap(find.toggleThemeButton());
    await pumpAndSettle();
  }

  CodeController findOneCodeController() {
    final codeField = find.snippetCodeField();
    expect(codeField, findsOneWidget);

    return widget<CodeField>(codeField).controller;
  }

  KeyedTabController<OutputTabEnum> findOutputTabController() {
    final beamTabBar = find.descendant(
      of: find.outputWidget(),
      matching: find.byType(BeamTabBar<OutputTabEnum>),
    );

    expect(beamTabBar, findsOneWidget);

    return beamTabBar.evaluate().first.getKeyedTabController<OutputTabEnum>()!;
  }

  String? findOutputText() {
    final codeField = widget(find.outputCodeField());
    return (codeField as CodeField).controller.text;
  }

  PlaygroundController findPlaygroundController() {
    final context = element(find.snippetCodeField());
    return context.read<PlaygroundController>();
  }

  Future<void> runShortcut(BeamShortcut shortcut) async {
    final list = shortcut.keys.toList();
    for (final key in list) {
      await sendKeyDownEvent(key);
    }
    for (final key in list.reversed) {
      await sendKeyUpEvent(key);
    }
  }

  Future<void> tapAndSettle(
    Finder finder, {
    bool warnIfMissed = true,
  }) async {
    await tap(
      finder,
      warnIfMissed: warnIfMissed,
    );
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

  /// Runs and expects that the execution is as fast as it should be for cache.
  Future<void> runExpectCached(ExampleDescriptor example) async {
    final codeRunner = findPlaygroundController().codeRunner;

    await tap(find.runOrCancelButton());
    await pump();
    expect(codeRunner.isCodeRunning, true);

    try {
      await pumpAndSettle(
        const Duration(milliseconds: 100),
        EnginePhase.sendSemanticsUpdate,
        const Duration(milliseconds: 1000),
      );

      // ignore: avoid_catching_errors
    } on FlutterError {
      // Expected timeout because for some reason UI updates way longer.
    }

    expect(codeRunner.isCodeRunning, false);
    expect(
      PlaygroundComponents.analyticsService.lastEvent,
      isA<RunFinishedAnalyticsEvent>(),
    );

    await pumpAndSettle(); // Let the UI catch up.

    expectOutputStartsWith(kCachedResultsLog, this);
    expectOutputIfDeployed(example, this);
  }

  Future<void> modifyRunExpectReal(ExampleDescriptor example) async {
    modifyCodeController();

    final playgroundController = findPlaygroundController();
    final eventSnippetContext = playgroundController.eventSnippetContext;
    expect(eventSnippetContext.snippet, null);

    await tap(find.runOrCancelButton());
    await pumpAndSettle();

    final actualText = findOutputText();
    expect(actualText, isNot(startsWith(kCachedResultsLog)));
    expectOutputIfDeployed(example, this);

    // Animation stops just before the analytics event is fired, wait a bit.
    await Future.delayed(const Duration(seconds: 1));

    final event = PlaygroundComponents.analyticsService.lastEvent;
    expect(event, isA<RunFinishedAnalyticsEvent>());

    final finishedEvent = event! as RunFinishedAnalyticsEvent;
    expect(finishedEvent.snippetContext, eventSnippetContext);
  }

  /// Modifies the code controller in a unique way by inserting timestamp
  /// in the comment.
  void modifyCodeController() {
    // Add a character into the first comment.
    // This relies on that the position 10 is inside a license comment.
    final controller = findOneCodeController();
    final text = controller.fullText;

    // ignore: prefer_interpolation_to_compose_strings
    controller.fullText = text.substring(0, 10) +
        DateTime.now().millisecondsSinceEpoch.toString() +
        text.substring(10);
  }

  Future<void> navigateAndSettle(String urlString) async {
    final url = Uri.parse(urlString);
    print('Navigating: $url\n'); // ignore: avoid_print
    await _navigate(url);

    // These appears to be the minimal reliable delay.
    await Future.delayed(const Duration(seconds: 3));
    await pumpAndSettle();
  }

  Future<void> _navigate(Uri uri) async {
    final parser = GetIt.instance.get<PageStackRouteInformationParser>();
    final path = await parser.parsePagePath(
      RouteInformation(location: uri.toString()),
    );

    if (path == null) {
      fail('Cannot parse $uri into PagePath');
    }

    final pageStack = GetIt.instance.get<PageStack>();
    await pageStack.replacePath(path, mode: PageStackMatchMode.none);
  }
}
