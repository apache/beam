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

import '../models/check_status_response.dart';
import '../models/output_response.dart';
import '../models/run_code_request.dart';
import '../models/run_code_response.dart';

abstract class CodeClient {
  Future<RunCodeResponse> runCode(RunCodeRequest request);

  Future<void> cancelExecution(String pipelineUuid);

  Future<CheckStatusResponse> checkStatus(
    String pipelineUuid,
  );

  Future<OutputResponse> getCompileOutput(
    String pipelineUuid,
  );

  Future<OutputResponse> getRunOutput(
    String pipelineUuid,
  );

  Future<OutputResponse> getLogOutput(
    String pipelineUuid,
  );

  Future<OutputResponse> getRunErrorOutput(
    String pipelineUuid,
  );

  Future<OutputResponse> getValidationErrorOutput(
    String pipelineUuid,
  );

  Future<OutputResponse> getPreparationErrorOutput(
    String pipelineUuid,
  );

  Future<OutputResponse> getGraphOutput(
    String pipelineUuid,
  );
}
