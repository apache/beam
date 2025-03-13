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

import 'package:easy_localization/easy_localization.dart';
import 'package:grpc/grpc.dart';

import '../../api/iis_workaround_channel.dart';
import '../../api/v1/api.pbgrpc.dart' as grpc;
import '../../models/sdk.dart';
import '../../util/pipeline_options.dart';
import '../../util/run_with_retry.dart';
import '../dataset_grpc_extension.dart';
import '../models/check_status_response.dart';
import '../models/output_response.dart';
import '../models/run_code_error.dart';
import '../models/run_code_request.dart';
import '../models/run_code_response.dart';
import '../models/run_code_result.dart';
import '../models/snippet_file_grpc_extension.dart';
import '../sdk_grpc_extension.dart';
import 'code_client.dart';

const kGeneralError = 'Failed to execute code';

class GrpcCodeClient implements CodeClient {
  final grpc.PlaygroundServiceClient _defaultClient;
  final Map<String, Uri> _runnerUrlsById;

  factory GrpcCodeClient({
    required Uri url,
    required Map<String, Uri> runnerUrlsById,
  }) {
    final channel = IisWorkaroundChannel.xhr(url);

    return GrpcCodeClient._(
      client: grpc.PlaygroundServiceClient(channel),
      runnerUrlsById: runnerUrlsById,
    );
  }

  GrpcCodeClient._({
    required grpc.PlaygroundServiceClient client,
    required Map<String, Uri> runnerUrlsById,
  })  : _defaultClient = client,
        _runnerUrlsById = runnerUrlsById;

  @override
  Future<grpc.GetMetadataResponse> getMetadata(Sdk sdk) async {
    final client = _createRunCodeClient(sdk);
    return _runSafely(
      () => client.getMetadata(
        grpc.GetMetadataRequest(),
      ),
    );
  }

  @override
  Future<RunCodeResponse> runCode(RunCodeRequest request) async {
    final client = _createRunCodeClient(request.sdk);
    final response = await _runSafely(
      () => client.runCode(_grpcRunCodeRequest(request)),
    );

    return RunCodeResponse(
      pipelineUuid: response.pipelineUuid,
    );
  }

  @override
  Future<void> cancelExecution(String pipelineUuid) {
    return _runSafely(
      () => _defaultClient.cancel(
        grpc.CancelRequest(pipelineUuid: pipelineUuid),
      ),
    );
  }

  @override
  Future<CheckStatusResponse> checkStatus(
    String pipelineUuid,
  ) async {
    return runWithRetry(
      () => _checkStatusWithRetry(pipelineUuid),
    );
  }

  Future<CheckStatusResponse> _checkStatusWithRetry(
    String pipelineUuid,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.checkStatus(
        grpc.CheckStatusRequest(pipelineUuid: pipelineUuid),
      ),
    );

    return CheckStatusResponse(
      status: _toClientStatus(response.status),
    );
  }

  @override
  Future<OutputResponse> getCompileOutput(
    String pipelineUuid,
  ) async {
    final response = await _runSafely(
      () => _defaultClient.getCompileOutput(
        grpc.GetCompileOutputRequest(pipelineUuid: pipelineUuid),
      ),
    );

    return OutputResponse(output: response.output);
  }

  @override
  Future<OutputResponse> getRunOutput(
    String pipelineUuid,
  ) async {
    try {
      final response = await _runSafely(
        () => _defaultClient.getRunOutput(
          grpc.GetRunOutputRequest(pipelineUuid: pipelineUuid),
        ),
      );

      return OutputResponse(output: response.output);
    } catch (ex) {
      print(ex);
      return OutputResponse(output: '');
    }
  }

  @override
  Future<OutputResponse> getLogOutput(
    String pipelineUuid,
  ) async {
    try {
      final response = await _defaultClient.getLogs(
        grpc.GetLogsRequest(pipelineUuid: pipelineUuid),
      );

      return OutputResponse(output: response.output);
    } catch (ex) {
      print(ex);
      return OutputResponse(output: '');
    }
  }

  @override
  Future<OutputResponse> getRunErrorOutput(
    String pipelineUuid,
  ) async {
    final response = await _defaultClient.getRunError(
      grpc.GetRunErrorRequest(pipelineUuid: pipelineUuid),
    );

    return OutputResponse(output: response.output);
  }

  @override
  Future<OutputResponse> getValidationErrorOutput(
    String pipelineUuid,
  ) async {
    final response = await _defaultClient.getValidationOutput(
      grpc.GetValidationOutputRequest(pipelineUuid: pipelineUuid),
    );

    return OutputResponse(output: response.output);
  }

  @override
  Future<OutputResponse> getPreparationErrorOutput(
    String pipelineUuid,
  ) async {
    final response = await _defaultClient.getPreparationOutput(
      grpc.GetPreparationOutputRequest(pipelineUuid: pipelineUuid),
    );

    return OutputResponse(output: response.output);
  }

  @override
  Future<OutputResponse> getGraphOutput(
    String pipelineUuid,
  ) async {
    try {
      final response = await _defaultClient.getGraph(
        grpc.GetGraphRequest(pipelineUuid: pipelineUuid),
      );

      return OutputResponse(output: response.graph);
    } catch (ex) {
      print(ex);
      return OutputResponse(output: '');
    }
  }

  Future<T> _runSafely<T>(Future<T> Function() invoke) async {
    try {
      return await invoke();
    } on GrpcError catch (error) {
      switch (error.code) {
        case StatusCode.unknown:
          // The default can be misleading for this.
          throw RunCodeError(message: 'errors.unknownError'.tr());
        case StatusCode.resourceExhausted:
          throw RunCodeResourceExhaustedError(message: error.message);
      }
      throw RunCodeError(message: error.message);
    } on Exception catch (_) {
      throw const RunCodeError();
    }
  }

  /// Run Code request should use different urls for each sdk
  /// instead of the default one, because we need to code
  /// sdk services for it
  grpc.PlaygroundServiceClient _createRunCodeClient(Sdk sdk) {
    final apiClientURL = _runnerUrlsById[sdk.id];

    if (apiClientURL == null) {
      throw Exception('Runner not found for ${sdk.id}');
    }

    final channel = IisWorkaroundChannel.xhr(apiClientURL);
    return grpc.PlaygroundServiceClient(channel);
  }

  grpc.RunCodeRequest _grpcRunCodeRequest(RunCodeRequest request) {
    return grpc.RunCodeRequest(
      datasets: request.datasets.map((e) => e.grpc),
      files: request.files.map((f) => f.grpc),
      pipelineOptions: pipelineOptionsToString(request.pipelineOptions),
      sdk: request.sdk.grpc,
    );
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
        return RunCodeStatus.cancelled;
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
}
