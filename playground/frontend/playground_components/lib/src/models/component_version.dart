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

// ignore_for_file: avoid_redundant_argument_values

import '../build_metadata.g.dart' as frontend_build_metadata;

class ComponentVersion {
  const ComponentVersion({
    this.beamSdkVersion,
    this.buildCommitHash,
    this.dateTime,
  });

  final String? beamSdkVersion;
  final String? buildCommitHash;
  final DateTime? dateTime;

  /// The version of the frontend.
  static final frontend = ComponentVersion(
    buildCommitHash: frontend_build_metadata.buildCommitHash,
    dateTime: frontend_build_metadata.buildCommitSecondsSinceEpoch == null
        ? null
        : DateTime.fromMillisecondsSinceEpoch(
            // ignore: avoid_dynamic_calls
            frontend_build_metadata.buildCommitSecondsSinceEpoch * 1000,
          ),
  );
}
