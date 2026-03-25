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

import '../../../models/sdk.dart';
import 'abstract.dart';
import 'constants.dart';

/// Snippet code is modified for the first time.
///
/// For multi-file snippets this fires up to one time on each modified file.
class SnippetModifiedAnalyticsEvent extends AnalyticsEvent {
  const SnippetModifiedAnalyticsEvent({
    required this.additionalParams,
    required this.fileName,
    required this.sdk,
    required this.snippet,
  }) : super(
          name: BeamAnalyticsEvents.snippetModified,
        );

  final Map<String, dynamic> additionalParams;
  final String fileName;
  final Sdk sdk;
  final String snippet;

  @override
  List<Object?> get props => [
        ...super.props,
        additionalParams,
        fileName,
        sdk,
        snippet,
      ];

  @override
  Map<String, dynamic> toJson() => {
        ...super.toJson(),
        EventParams.fileName: fileName,
        EventParams.sdk: sdk.id,
        EventParams.snippet: snippet,
        ...additionalParams,
      };
}
