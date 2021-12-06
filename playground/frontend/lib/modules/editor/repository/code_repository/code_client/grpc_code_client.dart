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

import 'package:grpc/grpc_web.dart';
import 'package:playground/api/iis_workaround_channel.dart';
import 'package:playground/api/v1/api.pbgrpc.dart' as grpc;
import 'package:playground/config.g.dart';
import 'package:playground/modules/editor/parsers/run_options_parser.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/check_status_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/code_client.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/output_response.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/run_code_response.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_error.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

const kGeneralError = 'Failed to execute code';

class GrpcCodeClient implements CodeClient {
  grpc.PlaygroundServiceClient createClient(SDK? sdk) {
    String apiClientURL = kApiClientURL;
    if (sdk != null) {
      apiClientURL = sdk.getRoute;
    }
    IisWorkaroundChannel channel = IisWorkaroundChannel.xhr(
      Uri.parse(apiClientURL),
    );
    return grpc.PlaygroundServiceClient(channel);
  }

  @override
  Future<RunCodeResponse> runCode(RunCodeRequestWrapper request) {
    return _runSafely(() => createClient(request.sdk)
        .runCode(_toGrpcRequest(request))
        .then((response) => RunCodeResponse(response.pipelineUuid)));
  }

  @override
  Future<CheckStatusResponse> checkStatus(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => createClient(request.sdk)
        .checkStatus(grpc.CheckStatusRequest(pipelineUuid: pipelineUuid))
        .then(
          (response) => CheckStatusResponse(_toClientStatus(response.status)),
        ));
  }

  @override
  Future<OutputResponse> getCompileOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => createClient(request.sdk)
        .getCompileOutput(
          grpc.GetCompileOutputRequest(pipelineUuid: pipelineUuid),
        )
        .then((response) => OutputResponse(response.output)));
  }

  @override
  Future<OutputResponse> getRunOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => createClient(request.sdk)
        .getRunOutput(grpc.GetRunOutputRequest(pipelineUuid: pipelineUuid))
        .then((response) => OutputResponse(response.output)));
  }

  @override
  Future<OutputResponse> getRunErrorOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => createClient(request.sdk)
        .getRunError(grpc.GetRunErrorRequest(pipelineUuid: pipelineUuid))
        .then((response) => OutputResponse(response.output)));
  }

  Future<T> _runSafely<T>(Future<T> Function() invoke) async {
    try {
      return await invoke();
    } on GrpcError catch (error) {
      throw RunCodeError(error.message);
    } on Exception catch (_) {
      throw RunCodeError(null);
    }
  }

  grpc.RunCodeRequest _toGrpcRequest(RunCodeRequestWrapper request) {
    return grpc.RunCodeRequest()
      ..code = request.code
      ..sdk = _getGrpcSdk(request.sdk)
      ..pipelineOptions = pipelineOptionsToString(request.pipelineOptions);
  }

  grpc.Sdk _getGrpcSdk(SDK sdk) {
    switch (sdk) {
      case SDK.java:
        return grpc.Sdk.SDK_JAVA;
      case SDK.go:
        return grpc.Sdk.SDK_GO;
      case SDK.python:
        return grpc.Sdk.SDK_PYTHON;
      case SDK.scio:
        return grpc.Sdk.SDK_SCIO;
    }
  }

  RunCodeStatus _toClientStatus(grpc.Status status) {
    switch (status) {
      case grpc.Status.STATUS_UNSPECIFIED:
        return RunCodeStatus.unspecified;
      case grpc.Status.STATUS_VALIDATING:
      case grpc.Status.STATUS_PREPARING:
        return RunCodeStatus.preparation;
      case grpc.Status.STATUS_COMPILING:
        return RunCodeStatus.compiling;
      case grpc.Status.STATUS_EXECUTING:
        return RunCodeStatus.executing;
      case grpc.Status.STATUS_CANCELED:
      case grpc.Status.STATUS_FINISHED:
        return RunCodeStatus.finished;
      case grpc.Status.STATUS_COMPILE_ERROR:
        return RunCodeStatus.compileError;
      case grpc.Status.STATUS_RUN_TIMEOUT:
        return RunCodeStatus.timeout;
      case grpc.Status.STATUS_RUN_ERROR:
        return RunCodeStatus.runError;
      case grpc.Status.STATUS_ERROR:
      case grpc.Status.STATUS_VALIDATION_ERROR:
      case grpc.Status.STATUS_PREPARATION_ERROR:
        return RunCodeStatus.unknownError;
    }
    return RunCodeStatus.unspecified;
  }
}
