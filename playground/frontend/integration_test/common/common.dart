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
import 'package:flutter/cupertino.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/main.dart' as app;
import 'package:playground/router/route_information_parser.dart';
import 'package:playground_components/playground_components.dart';

Future<void> init(WidgetTester wt) async {
  app.main();
  await wt.pumpAndSettle();
}

void expectHasDescendant(Finder ancestor, Finder descendant) {
  expect(
    find.descendant(of: ancestor, matching: descendant),
    findsOneWidget,
  );
}

void expectSimilar(double a, double b) {
  Matcher closeToFraction(num value, double fraction) =>
      closeTo(value, value * fraction);
  Matcher onePerCentTolerance(num value) => closeToFraction(value, 0.01);
  expect(a, onePerCentTolerance(b));
}

final exampleUrls = {
  // TODO: Replace with 'apache/beam' before PR.
  Sdk.go:
      'https://raw.githubusercontent.com/akvelon/beam/issue24959_test-loading-url/playground/frontend/integration_test/common/examples/example.go',
};

Uri getExampleUrl(Sdk sdk) {
  final params = {
    'sdk': sdk.id,
    'url': exampleUrls[sdk],
    'readonly': 'readonly1,readonly2',
    'show': 'show',
    'unfold': 'unfold1,unfold2',
  };

  return Uri(
    path: '/',
    queryParameters: params,
  );
}
