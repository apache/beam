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

enum RunCodeStatus {
  unspecified,
  preparation,
  preparationError,
  validationError,
  compiling,
  compileError,
  executing,
  runError,
  finished,
  timeout,
  unknownError,
}

const kFinishedStatuses = [
  RunCodeStatus.unknownError,
  RunCodeStatus.timeout,
  RunCodeStatus.compileError,
  RunCodeStatus.runError,
  RunCodeStatus.validationError,
  RunCodeStatus.preparationError,
  RunCodeStatus.finished,
];

class RunCodeResult {
  final RunCodeStatus status;
  final String? output;
  final String? log;
  final String? errorMessage;

  RunCodeResult({
    required this.status,
    this.output,
    this.log,
    this.errorMessage,
  });

  bool get isFinished {
    return kFinishedStatuses.contains(status);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is RunCodeResult &&
          runtimeType == other.runtimeType &&
          status == other.status &&
          output == other.output &&
          log == other.log &&
          errorMessage == other.errorMessage;

  @override
  int get hashCode =>
      status.hashCode ^ output.hashCode ^ log.hashCode ^ errorMessage.hashCode;

  @override
  String toString() {
    return 'RunCodeResult{status: $status, output: $output, log: $log, errorMessage: $errorMessage}';
  }
}
