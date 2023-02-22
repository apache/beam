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

class BeamAnalyticsEvents {
  static const cancelRun = 'cancel_run';
  static const externalUrlNavigated = 'external_url_navigated';
  static const feedbackForm = 'feedback_form';
  static const rated = 'rated';
  static const reportIssue = 'report_issue';
  static const resetSnippet = 'reset_snippet';
  static const run = 'run';
  static const runFinished = 'run_finished';
  static const sdkSelected = 'sdk_selected';
  static const snippetModified = 'snippet_modified';
  static const themeSet = 'theme_set';
}

class EventParams {
  static const app = 'app';
  static const brightness = 'brightness';
  static const destinationUrl = 'destinationUrl';
  static const feedbackRating = 'feedbackRating';
  static const feedbackText = 'feedbackText';
  static const fileName = 'fileName';
  static const runSeconds = 'runSeconds';
  static const sdk = 'sdk';
  static const snippet = 'snippet';
}
