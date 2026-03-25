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

class BeamIntent extends Intent {
  final String slug;

  const BeamIntent({
    required this.slug,
  });
}

class ResetIntent extends BeamIntent {
  const ResetIntent() : super(slug: 'intents.playground.reset');
}

class RunIntent extends BeamIntent {
  const RunIntent() : super(slug: 'intents.playground.run');
}

class ShowSuggestionsIntent extends BeamIntent {
  const ShowSuggestionsIntent()
      : super(slug: 'intents.playground.showSuggestions');
}
