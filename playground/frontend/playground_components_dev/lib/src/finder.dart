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

extension FinderExtension on Finder {
  // TODO(alexeyinkin): Push to Flutter or wait for them to make their own, https://github.com/flutter/flutter/issues/117675
  Finder and(Finder another) {
    return _AndFinder(this, another);
  }
}

class _AndFinder extends ChainedFinder {
  _AndFinder(super.parent, this.another);

  final Finder another;

  @override
  String get description => '${parent.description} AND ${another.description}';

  @override
  Iterable<Element> filter(Iterable<Element> parentCandidates) {
    return another.apply(parentCandidates);
  }
}
