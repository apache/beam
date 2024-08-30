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

import '../../models/sdk.dart';

enum RunCodeStatus {
  cancelled,
  compileError,
  compiling,
  executing,
  finished,
  preparation,
  preparationError,
  runError,
  timeout,
  unknownError,
  unspecified,
  validationError,
}

const kFinishedStatuses = {
  RunCodeStatus.cancelled,
  RunCodeStatus.compileError,
  RunCodeStatus.finished,
  RunCodeStatus.preparationError,
  RunCodeStatus.runError,
  RunCodeStatus.timeout,
  RunCodeStatus.unknownError,
  RunCodeStatus.validationError,
};

class RunCodeResult with EquatableMixin {
  final String? errorMessage;
  final String? graph;
  final String? log;
  final String? output;
  final String? pipelineUuid;
  final Sdk sdk;
  final RunCodeStatus status;

  const RunCodeResult({
    required this.sdk,
    required this.status,
    this.errorMessage,
    this.graph,
    this.log,
    this.output,
    this.pipelineUuid,
  });

  bool get isFinished {
    return kFinishedStatuses.contains(status);
  }

  @override
  List<Object?> get props => [
        errorMessage,
        graph,
        log,
        output,
        pipelineUuid,
        sdk,
        status,
      ];
}
