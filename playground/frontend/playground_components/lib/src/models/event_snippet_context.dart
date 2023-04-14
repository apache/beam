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

import 'package:equatable/equatable.dart';

import 'sdk.dart';

const _none = 'none';

/// Basic information of the Playground state at a particular moment of time
/// to augment analytics events.
class EventSnippetContext with EquatableMixin {
  const EventSnippetContext({
    required this.originalSnippet,
    required this.sdk,
    required this.snippet,
  });

  /// Any identifier of the snippet that the content was derived from.
  ///
  /// If the code was modified it is still the last snippet loaded or chosen
  /// by the user.
  final String? originalSnippet;

  final Sdk? sdk;

  /// Any identifier of the current snippet if it is unchanged.
  final String? snippet;

  /// An empty [EventSnippetContext] for events before anything loaded.
  static const empty = EventSnippetContext(
    originalSnippet: null,
    sdk: null,
    snippet: null,
  );

  @override
  List<Object?> get props => [
        originalSnippet,
        sdk?.id,
        snippet,
      ];

  Map<String, dynamic> toJson() => {
        'originalSnippet': originalSnippet ?? _none,
        'sdk': sdk?.id ?? _none,
        'snippet': snippet ?? _none,
      };
}
