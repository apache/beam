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
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import 'common_finders.dart';
import 'expect.dart';

extension WidgetTesterExtension on WidgetTester {
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

  /// Runs and expects that the execution is as fast as it should be for cache.
  Future<void> runExpectCached() async {
    final dateTimeStart = DateTime.now();

    await tap(find.runOrCancelButton());
    await pumpAndSettle();

    expectOutputStartsWith(kCachedResultsLog, this);
    expect(
      DateTime.now().difference(dateTimeStart),
      /// This is the safe threshold considering test delays.
      lessThan(const Duration(milliseconds: 3000)),
    );
  }

  /// Runs and expects that the execution is as fast as it should be for cache.
  Future<void> runExpectReal() async {
    await tap(find.runOrCancelButton());
    await pumpAndSettle();

    final actualText = findOutputText();
    expect(actualText, isNot(startsWith(kCachedResultsLog)));
  }

  Future<void> navigateAndSettle(String urlString) async {
    final url = Uri.parse(urlString);
    print('Navigating: $url\n');
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
