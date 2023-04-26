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

import '../../../enums/feedback_rating.dart';
import 'abstract.dart';
import 'constants.dart';

/// A thump-up or thumb-down button pressed without yet entering feedback text.
class AppRatedAnalyticsEvent extends AnalyticsEventWithSnippetContext {
  const AppRatedAnalyticsEvent({
    required this.rating,
    required super.snippetContext,
    super.additionalParams,
  }) : super(
          name: BeamAnalyticsEvents.appRated,
        );

  final FeedbackRating rating;

  @override
  List<Object?> get props => [
        ...super.props,
        rating,
      ];

  @override
  Map<String, dynamic> toJson() => {
        ...super.toJson(),
        EventParams.feedbackRating: rating.name,
      };
}
