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
import 'package:playground/utils/replace_incorrect_symbols.dart';

const kGeneralError = 'Failed to execute code';

class GrpcCodeClient implements CodeClient {
  late final grpc.PlaygroundServiceClient _defaultClient;

  GrpcCodeClient() {
    final channel = IisWorkaroundChannel.xhr(
      Uri.parse(kApiClientURL),
    );
    _defaultClient = grpc.PlaygroundServiceClient(channel);
  }

  @override
  Future<RunCodeResponse> runCode(RunCodeRequestWrapper request) {
    return _runSafely(() => _createRunCodeClient(request.sdk)
        .runCode(_toGrpcRequest(request))
        .then((response) => RunCodeResponse(response.pipelineUuid)));
  }

  @override
  Future<void> cancelExecution(String pipelineUuid) {
    return _runSafely(() =>
        _defaultClient.cancel(grpc.CancelRequest(pipelineUuid: pipelineUuid)));
  }

  @override
  Future<CheckStatusResponse> checkStatus(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
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
    return _runSafely(() => _defaultClient
        .getCompileOutput(
          grpc.GetCompileOutputRequest(pipelineUuid: pipelineUuid),
        )
        .then((response) => _toOutputResponse(response.output)));
  }

  @override
  Future<OutputResponse> getRunOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
            .getRunOutput(grpc.GetRunOutputRequest(pipelineUuid: pipelineUuid))
            .then((response) => _toOutputResponse(response.output))
            .catchError((err) {
          print(err);
          return _toOutputResponse('');
        }));
  }

  @override
  Future<OutputResponse> getLogOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
            .getLogs(grpc.GetLogsRequest(pipelineUuid: pipelineUuid))
            .then((response) => _toOutputResponse(response.output))
            .catchError((err) {
          print(err);
          return _toOutputResponse('');
        }));
  }

  @override
  Future<OutputResponse> getRunErrorOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
        .getRunError(grpc.GetRunErrorRequest(pipelineUuid: pipelineUuid))
        .then((response) => _toOutputResponse(response.output)));
  }

  @override
  Future<OutputResponse> getValidationErrorOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
        .getValidationOutput(
            grpc.GetValidationOutputRequest(pipelineUuid: pipelineUuid))
        .then((response) => _toOutputResponse(response.output)));
  }

  @override
  Future<OutputResponse> getPreparationErrorOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
        .getPreparationOutput(
            grpc.GetPreparationOutputRequest(pipelineUuid: pipelineUuid))
        .then((response) => _toOutputResponse(response.output)));
  }

  @override
  Future<OutputResponse> getGraphOutput(
    String pipelineUuid,
    RunCodeRequestWrapper request,
  ) {
    return _runSafely(() => _defaultClient
            .getGraph(grpc.GetGraphRequest(pipelineUuid: pipelineUuid))
            .then((response) => OutputResponse(response.graph))
            .catchError((err) {
          print(err);
          return _toOutputResponse('');
        }));
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

  /// Run Code request should use different urls for each sdk
  /// instead of the default one, because we need to code
  /// sdk services for it
  grpc.PlaygroundServiceClient _createRunCodeClient(SDK? sdk) {
    String apiClientURL = kApiClientURL;
    if (sdk != null) {
      apiClientURL = sdk.getRoute;
    }
    IisWorkaroundChannel channel = IisWorkaroundChannel.xhr(
      Uri.parse(apiClientURL),
    );
    return grpc.PlaygroundServiceClient(channel);
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
      case grpc.Status.STATUS_VALIDATION_ERROR:
        return RunCodeStatus.validationError;
      case grpc.Status.STATUS_PREPARATION_ERROR:
        return RunCodeStatus.preparationError;
      case grpc.Status.STATUS_ERROR:
        return RunCodeStatus.unknownError;
    }
    return RunCodeStatus.unspecified;
  }

  OutputResponse _toOutputResponse(String response) {
    return OutputResponse(replaceIncorrectSymbols(response));
  }
}
