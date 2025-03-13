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

import '../../../models/event_snippet_context.dart';

/// The root event class for analytics.
abstract class AnalyticsEvent with EquatableMixin {
  const AnalyticsEvent({
    required this.name,
  });

  final String name;

  @override
  List<Object?> get props => [
        name,
      ];

  /// Returns the event params to be sent.
  Map<String, dynamic> toJson() => const {};
}

/// An [AnalyticsEvent] with [EventSnippetContext].
abstract class AnalyticsEventWithSnippetContext extends AnalyticsEvent {
  const AnalyticsEventWithSnippetContext({
    required this.snippetContext,
    required super.name,
    this.additionalParams = const {},
  });

  final EventSnippetContext? snippetContext;
  final Map<String, dynamic> additionalParams;

  @override
  List<Object?> get props => [
        ...super.props,
        snippetContext,
      ];

  @override
  Map<String, dynamic> toJson() {
    final map = {
      ...super.toJson(),
      ...?snippetContext?.toJson(),
      ...additionalParams,
    };
    return map;
  }
}
