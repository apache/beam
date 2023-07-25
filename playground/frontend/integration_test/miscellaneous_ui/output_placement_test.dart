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

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/modules/output/models/output_placement.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import '../common/common_finders.dart';

Future<void> checkOutputPlacement(WidgetTester wt) async {
  Offset getCodeAreaCenter() => wt.getCenter(find.codeTextAreaWrapper());
  Offset getOutputCenter() => wt.getCenter(find.outputWidget());

  await wt.tapAndSettle(find.byKey(const ValueKey(OutputPlacement.left)));
  expect(getCodeAreaCenter().dx > getOutputCenter().dx, true);
  expectSimilar(getCodeAreaCenter().dy, getOutputCenter().dy);

  await wt.tapAndSettle(find.byKey(const ValueKey(OutputPlacement.right)));
  expect(getCodeAreaCenter().dx < getOutputCenter().dx, true);
  expectSimilar(getCodeAreaCenter().dy, getOutputCenter().dy);

  await wt.tapAndSettle(find.byKey(const ValueKey(OutputPlacement.bottom)));
  expect(getCodeAreaCenter().dy < getOutputCenter().dy, true);
  expectSimilar(getCodeAreaCenter().dx, getOutputCenter().dx);
}
