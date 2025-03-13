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

import 'dart:ui';

import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/playground_components.dart';

import 'examples/example_descriptor.dart';
import 'widget_tester.dart';

void expectContextLine(
  int contextLine1Based,
  WidgetTester wt, {
  String? reason,
}) {
  final controller = wt.findOneCodeController();
  final selection = controller.selection;
  final position = controller.code.hiddenRanges.recoverPosition(
    selection.baseOffset,
    placeHiddenRanges: TextAffinity.downstream,
  );

  expect(selection.isCollapsed, true);
  expect(
    controller.code.lines.characterIndexToLineIndex(position),
    contextLine1Based - 1,
    reason: reason,
  );
}

void expectOutputIfDeployed(ExampleDescriptor example, WidgetTester wt) {
  if (example.outputTail != null) {
    expectOutputEndsWithIfDeployed(example.outputTail, wt);
  } else if (example.outputContains != null) {
    for (final str in example.outputContains!) {
      expectOutputContainsIfDeployed(str, wt);
    }
  } else {
    throw AssertionError('No pattern to check example output: ${example.path}');
  }
}

void expectOutputEqualsIfDeployed(String text, WidgetTester wt) {
  if (!areExamplesDeployed) {
    return;
  }

  final actualText = wt.findOutputText();
  expect(actualText, text);
}

void expectOutputContainsIfDeployed(String? text, WidgetTester wt) {
  if (!areExamplesDeployed) {
    return;
  }

  final actualText = wt.findOutputText();
  expect(text, isNotNull);
  expect(actualText, contains(text));
}

void expectOutputEndsWithIfDeployed(String? text, WidgetTester wt) {
  if (!areExamplesDeployed) {
    return;
  }

  final actualText = wt.findOutputText();
  expect(text, isNotNull);
  expect(actualText, endsWith(text!));
}

void expectOutputStartsWith(String? text, WidgetTester wt) {
  final actualText = wt.findOutputText();
  expect(text, isNotNull);
  expect(actualText, startsWith(text!));
}

void expectSdk(Sdk sdk, WidgetTester wt) {
  final controller = wt.findPlaygroundController();
  expect(controller.sdk, sdk);
}

void expectSimilar(double a, double b) {
  Matcher closeToFraction(num value, double fraction) =>
      closeTo(value, value * fraction);
  Matcher onePerCentTolerance(num value) => closeToFraction(value, 0.01);
  expect(a, onePerCentTolerance(b));
}

void expectVisibleTextIfDeployed(String? visibleText, WidgetTester wt) {
  if (!areExamplesDeployed) {
    return;
  }

  expectVisibleText(visibleText, wt);
}

void expectVisibleText(String? visibleText, WidgetTester wt, {String? reason}) {
  final controller = wt.findOneCodeController();
  expect(visibleText, isNotNull);
  expect(controller.text, visibleText, reason: reason);
}

void expectLastAnalyticsEvent(
  AnalyticsEvent event, {
  String? reason,
}) {
  expect(
    PlaygroundComponents.analyticsService.lastEvent,
    event,
    reason: reason,
  );
}
