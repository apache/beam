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

import 'example_base.dart';

const _pinnedTitle = 'quick start';

class CategoryWithExamples implements Comparable<CategoryWithExamples> {
  // TODO(alexeyinkin): Sort on the backend instead, then make const constructor, https://github.com/apache/beam/issues/23083
  final bool isPinned;
  final String title;
  final List<ExampleBase> examples;

  CategoryWithExamples({
    required this.title,
    required this.examples,
  }) : isPinned = title.toLowerCase() == _pinnedTitle;

  @override
  int compareTo(CategoryWithExamples other) {
    if (isPinned && !other.isPinned) {
      return -1;
    }

    if (!isPinned && other.isPinned) {
      return 1;
    }

    return title.toLowerCase().compareTo(other.title.toLowerCase());
  }
}
