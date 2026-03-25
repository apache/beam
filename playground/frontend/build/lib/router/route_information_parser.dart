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

// ignore_for_file: avoid_renaming_method_parameters
// ignore_for_file: unnecessary_null_in_if_null_operators

import 'package:app_state/app_state.dart';
import 'package:flutter/widgets.dart';

import '../pages/embedded_playground/path.dart';
import '../pages/standalone_playground/path.dart';

/// Converts URL strings into [PagePath] objects to construct pages later.
class PlaygroundRouteInformationParser extends PageStackRouteInformationParser {
  @override
  Future<PagePath> parsePagePath(RouteInformation ri) async {
    final uri = Uri.parse(ri.location ?? '');

    // null is for better auto-formatting below.
    return null ??

        // Embedded:
        EmbeddedPlaygroundMultiplePath.tryParse(uri) ??
        EmbeddedPlaygroundSingleFirstPath.tryParse(uri) ??
        EmbeddedPlaygroundEmptyPath.tryParse(uri) ??

        // Standalone:
        StandalonePlaygroundMultiplePath.tryParse(uri) ??
        StandalonePlaygroundSingleFirstPath.tryParse(uri) ??
        StandalonePlaygroundWaitPath.tryParse(uri) ??
        StandalonePlaygroundDefaultPath.parse(uri);
  }
}
