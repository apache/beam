///
//  Generated code. Do not modify.
//  source: api/v1/api.proto
//
// @dart = 2.12
// ignore_for_file: annotate_overrides,camel_case_types,constant_identifier_names,directives_ordering,library_prefixes,non_constant_identifier_names,prefer_final_fields,return_of_invalid_type,unnecessary_const,unnecessary_import,unnecessary_this,unused_import,unused_shown_name

import 'dart:async' as $async;

import 'dart:core' as $core;

import 'package:grpc/service_api.dart' as $grpc;
import 'api.pb.dart' as $0;
export 'api.pb.dart';

class PlaygroundServiceClient extends $grpc.Client {
  static final _$runCode =
      $grpc.ClientMethod<$0.RunCodeRequest, $0.RunCodeResponse>(
          '/api.v1.PlaygroundService/RunCode',
          ($0.RunCodeRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.RunCodeResponse.fromBuffer(value));
  static final _$checkStatus =
      $grpc.ClientMethod<$0.CheckStatusRequest, $0.CheckStatusResponse>(
          '/api.v1.PlaygroundService/CheckStatus',
          ($0.CheckStatusRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.CheckStatusResponse.fromBuffer(value));
  static final _$getRunOutput =
      $grpc.ClientMethod<$0.GetRunOutputRequest, $0.GetRunOutputResponse>(
          '/api.v1.PlaygroundService/GetRunOutput',
          ($0.GetRunOutputRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetRunOutputResponse.fromBuffer(value));
  static final _$getLogs =
      $grpc.ClientMethod<$0.GetLogsRequest, $0.GetLogsResponse>(
          '/api.v1.PlaygroundService/GetLogs',
          ($0.GetLogsRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetLogsResponse.fromBuffer(value));
  static final _$getGraph =
      $grpc.ClientMethod<$0.GetGraphRequest, $0.GetGraphResponse>(
          '/api.v1.PlaygroundService/GetGraph',
          ($0.GetGraphRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetGraphResponse.fromBuffer(value));
  static final _$getRunError =
      $grpc.ClientMethod<$0.GetRunErrorRequest, $0.GetRunErrorResponse>(
          '/api.v1.PlaygroundService/GetRunError',
          ($0.GetRunErrorRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetRunErrorResponse.fromBuffer(value));
  static final _$getValidationOutput = $grpc.ClientMethod<
          $0.GetValidationOutputRequest, $0.GetValidationOutputResponse>(
      '/api.v1.PlaygroundService/GetValidationOutput',
      ($0.GetValidationOutputRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetValidationOutputResponse.fromBuffer(value));
  static final _$getPreparationOutput = $grpc.ClientMethod<
          $0.GetPreparationOutputRequest, $0.GetPreparationOutputResponse>(
      '/api.v1.PlaygroundService/GetPreparationOutput',
      ($0.GetPreparationOutputRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPreparationOutputResponse.fromBuffer(value));
  static final _$getCompileOutput = $grpc.ClientMethod<
          $0.GetCompileOutputRequest, $0.GetCompileOutputResponse>(
      '/api.v1.PlaygroundService/GetCompileOutput',
      ($0.GetCompileOutputRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetCompileOutputResponse.fromBuffer(value));
  static final _$cancel =
      $grpc.ClientMethod<$0.CancelRequest, $0.CancelResponse>(
          '/api.v1.PlaygroundService/Cancel',
          ($0.CancelRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) => $0.CancelResponse.fromBuffer(value));
  static final _$getPrecompiledObjects = $grpc.ClientMethod<
          $0.GetPrecompiledObjectsRequest, $0.GetPrecompiledObjectsResponse>(
      '/api.v1.PlaygroundService/GetPrecompiledObjects',
      ($0.GetPrecompiledObjectsRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPrecompiledObjectsResponse.fromBuffer(value));
  static final _$getPrecompiledObject = $grpc.ClientMethod<
          $0.GetPrecompiledObjectRequest, $0.GetPrecompiledObjectResponse>(
      '/api.v1.PlaygroundService/GetPrecompiledObject',
      ($0.GetPrecompiledObjectRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPrecompiledObjectResponse.fromBuffer(value));
  static final _$getPrecompiledObjectCode = $grpc.ClientMethod<
          $0.GetPrecompiledObjectCodeRequest,
          $0.GetPrecompiledObjectCodeResponse>(
      '/api.v1.PlaygroundService/GetPrecompiledObjectCode',
      ($0.GetPrecompiledObjectCodeRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPrecompiledObjectCodeResponse.fromBuffer(value));
  static final _$getPrecompiledObjectOutput = $grpc.ClientMethod<
          $0.GetPrecompiledObjectOutputRequest,
          $0.GetPrecompiledObjectOutputResponse>(
      '/api.v1.PlaygroundService/GetPrecompiledObjectOutput',
      ($0.GetPrecompiledObjectOutputRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPrecompiledObjectOutputResponse.fromBuffer(value));
  static final _$getPrecompiledObjectLogs = $grpc.ClientMethod<
          $0.GetPrecompiledObjectLogsRequest,
          $0.GetPrecompiledObjectLogsResponse>(
      '/api.v1.PlaygroundService/GetPrecompiledObjectLogs',
      ($0.GetPrecompiledObjectLogsRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPrecompiledObjectLogsResponse.fromBuffer(value));
  static final _$getPrecompiledObjectGraph = $grpc.ClientMethod<
          $0.GetPrecompiledObjectGraphRequest,
          $0.GetPrecompiledObjectGraphResponse>(
      '/api.v1.PlaygroundService/GetPrecompiledObjectGraph',
      ($0.GetPrecompiledObjectGraphRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetPrecompiledObjectGraphResponse.fromBuffer(value));
  static final _$getDefaultPrecompiledObject = $grpc.ClientMethod<
          $0.GetDefaultPrecompiledObjectRequest,
          $0.GetDefaultPrecompiledObjectResponse>(
      '/api.v1.PlaygroundService/GetDefaultPrecompiledObject',
      ($0.GetDefaultPrecompiledObjectRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetDefaultPrecompiledObjectResponse.fromBuffer(value));
  static final _$saveSnippet =
      $grpc.ClientMethod<$0.SaveSnippetRequest, $0.SaveSnippetResponse>(
          '/api.v1.PlaygroundService/SaveSnippet',
          ($0.SaveSnippetRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.SaveSnippetResponse.fromBuffer(value));
  static final _$getSnippet =
      $grpc.ClientMethod<$0.GetSnippetRequest, $0.GetSnippetResponse>(
          '/api.v1.PlaygroundService/GetSnippet',
          ($0.GetSnippetRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetSnippetResponse.fromBuffer(value));
  static final _$getMetadata =
      $grpc.ClientMethod<$0.GetMetadataRequest, $0.GetMetadataResponse>(
          '/api.v1.PlaygroundService/GetMetadata',
          ($0.GetMetadataRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetMetadataResponse.fromBuffer(value));

  PlaygroundServiceClient($grpc.ClientChannel channel,
      {$grpc.CallOptions? options,
      $core.Iterable<$grpc.ClientInterceptor>? interceptors})
      : super(channel, options: options, interceptors: interceptors);

  $grpc.ResponseFuture<$0.RunCodeResponse> runCode($0.RunCodeRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$runCode, request, options: options);
  }

  $grpc.ResponseFuture<$0.CheckStatusResponse> checkStatus(
      $0.CheckStatusRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$checkStatus, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetRunOutputResponse> getRunOutput(
      $0.GetRunOutputRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getRunOutput, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetLogsResponse> getLogs($0.GetLogsRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getLogs, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetGraphResponse> getGraph($0.GetGraphRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getGraph, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetRunErrorResponse> getRunError(
      $0.GetRunErrorRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getRunError, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetValidationOutputResponse> getValidationOutput(
      $0.GetValidationOutputRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getValidationOutput, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetPreparationOutputResponse> getPreparationOutput(
      $0.GetPreparationOutputRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPreparationOutput, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetCompileOutputResponse> getCompileOutput(
      $0.GetCompileOutputRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getCompileOutput, request, options: options);
  }

  $grpc.ResponseFuture<$0.CancelResponse> cancel($0.CancelRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$cancel, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetPrecompiledObjectsResponse> getPrecompiledObjects(
      $0.GetPrecompiledObjectsRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPrecompiledObjects, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetPrecompiledObjectResponse> getPrecompiledObject(
      $0.GetPrecompiledObjectRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPrecompiledObject, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetPrecompiledObjectCodeResponse>
      getPrecompiledObjectCode($0.GetPrecompiledObjectCodeRequest request,
          {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPrecompiledObjectCode, request,
        options: options);
  }

  $grpc.ResponseFuture<$0.GetPrecompiledObjectOutputResponse>
      getPrecompiledObjectOutput($0.GetPrecompiledObjectOutputRequest request,
          {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPrecompiledObjectOutput, request,
        options: options);
  }

  $grpc.ResponseFuture<$0.GetPrecompiledObjectLogsResponse>
      getPrecompiledObjectLogs($0.GetPrecompiledObjectLogsRequest request,
          {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPrecompiledObjectLogs, request,
        options: options);
  }

  $grpc.ResponseFuture<$0.GetPrecompiledObjectGraphResponse>
      getPrecompiledObjectGraph($0.GetPrecompiledObjectGraphRequest request,
          {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getPrecompiledObjectGraph, request,
        options: options);
  }

  $grpc.ResponseFuture<$0.GetDefaultPrecompiledObjectResponse>
      getDefaultPrecompiledObject($0.GetDefaultPrecompiledObjectRequest request,
          {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getDefaultPrecompiledObject, request,
        options: options);
  }

  $grpc.ResponseFuture<$0.SaveSnippetResponse> saveSnippet(
      $0.SaveSnippetRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$saveSnippet, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetSnippetResponse> getSnippet(
      $0.GetSnippetRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getSnippet, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetMetadataResponse> getMetadata(
      $0.GetMetadataRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getMetadata, request, options: options);
  }
}

abstract class PlaygroundServiceBase extends $grpc.Service {
  $core.String get $name => 'api.v1.PlaygroundService';

  PlaygroundServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.RunCodeRequest, $0.RunCodeResponse>(
        'RunCode',
        runCode_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.RunCodeRequest.fromBuffer(value),
        ($0.RunCodeResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.CheckStatusRequest, $0.CheckStatusResponse>(
            'CheckStatus',
            checkStatus_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CheckStatusRequest.fromBuffer(value),
            ($0.CheckStatusResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.GetRunOutputRequest, $0.GetRunOutputResponse>(
            'GetRunOutput',
            getRunOutput_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.GetRunOutputRequest.fromBuffer(value),
            ($0.GetRunOutputResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetLogsRequest, $0.GetLogsResponse>(
        'GetLogs',
        getLogs_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.GetLogsRequest.fromBuffer(value),
        ($0.GetLogsResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetGraphRequest, $0.GetGraphResponse>(
        'GetGraph',
        getGraph_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.GetGraphRequest.fromBuffer(value),
        ($0.GetGraphResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.GetRunErrorRequest, $0.GetRunErrorResponse>(
            'GetRunError',
            getRunError_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.GetRunErrorRequest.fromBuffer(value),
            ($0.GetRunErrorResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetValidationOutputRequest,
            $0.GetValidationOutputResponse>(
        'GetValidationOutput',
        getValidationOutput_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetValidationOutputRequest.fromBuffer(value),
        ($0.GetValidationOutputResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPreparationOutputRequest,
            $0.GetPreparationOutputResponse>(
        'GetPreparationOutput',
        getPreparationOutput_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPreparationOutputRequest.fromBuffer(value),
        ($0.GetPreparationOutputResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetCompileOutputRequest,
            $0.GetCompileOutputResponse>(
        'GetCompileOutput',
        getCompileOutput_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetCompileOutputRequest.fromBuffer(value),
        ($0.GetCompileOutputResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CancelRequest, $0.CancelResponse>(
        'Cancel',
        cancel_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.CancelRequest.fromBuffer(value),
        ($0.CancelResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPrecompiledObjectsRequest,
            $0.GetPrecompiledObjectsResponse>(
        'GetPrecompiledObjects',
        getPrecompiledObjects_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPrecompiledObjectsRequest.fromBuffer(value),
        ($0.GetPrecompiledObjectsResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPrecompiledObjectRequest,
            $0.GetPrecompiledObjectResponse>(
        'GetPrecompiledObject',
        getPrecompiledObject_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPrecompiledObjectRequest.fromBuffer(value),
        ($0.GetPrecompiledObjectResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPrecompiledObjectCodeRequest,
            $0.GetPrecompiledObjectCodeResponse>(
        'GetPrecompiledObjectCode',
        getPrecompiledObjectCode_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPrecompiledObjectCodeRequest.fromBuffer(value),
        ($0.GetPrecompiledObjectCodeResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPrecompiledObjectOutputRequest,
            $0.GetPrecompiledObjectOutputResponse>(
        'GetPrecompiledObjectOutput',
        getPrecompiledObjectOutput_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPrecompiledObjectOutputRequest.fromBuffer(value),
        ($0.GetPrecompiledObjectOutputResponse value) =>
            value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPrecompiledObjectLogsRequest,
            $0.GetPrecompiledObjectLogsResponse>(
        'GetPrecompiledObjectLogs',
        getPrecompiledObjectLogs_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPrecompiledObjectLogsRequest.fromBuffer(value),
        ($0.GetPrecompiledObjectLogsResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetPrecompiledObjectGraphRequest,
            $0.GetPrecompiledObjectGraphResponse>(
        'GetPrecompiledObjectGraph',
        getPrecompiledObjectGraph_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetPrecompiledObjectGraphRequest.fromBuffer(value),
        ($0.GetPrecompiledObjectGraphResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetDefaultPrecompiledObjectRequest,
            $0.GetDefaultPrecompiledObjectResponse>(
        'GetDefaultPrecompiledObject',
        getDefaultPrecompiledObject_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetDefaultPrecompiledObjectRequest.fromBuffer(value),
        ($0.GetDefaultPrecompiledObjectResponse value) =>
            value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.SaveSnippetRequest, $0.SaveSnippetResponse>(
            'SaveSnippet',
            saveSnippet_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.SaveSnippetRequest.fromBuffer(value),
            ($0.SaveSnippetResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetSnippetRequest, $0.GetSnippetResponse>(
        'GetSnippet',
        getSnippet_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.GetSnippetRequest.fromBuffer(value),
        ($0.GetSnippetResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.GetMetadataRequest, $0.GetMetadataResponse>(
            'GetMetadata',
            getMetadata_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.GetMetadataRequest.fromBuffer(value),
            ($0.GetMetadataResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.RunCodeResponse> runCode_Pre(
      $grpc.ServiceCall call, $async.Future<$0.RunCodeRequest> request) async {
    return runCode(call, await request);
  }

  $async.Future<$0.CheckStatusResponse> checkStatus_Pre($grpc.ServiceCall call,
      $async.Future<$0.CheckStatusRequest> request) async {
    return checkStatus(call, await request);
  }

  $async.Future<$0.GetRunOutputResponse> getRunOutput_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetRunOutputRequest> request) async {
    return getRunOutput(call, await request);
  }

  $async.Future<$0.GetLogsResponse> getLogs_Pre(
      $grpc.ServiceCall call, $async.Future<$0.GetLogsRequest> request) async {
    return getLogs(call, await request);
  }

  $async.Future<$0.GetGraphResponse> getGraph_Pre(
      $grpc.ServiceCall call, $async.Future<$0.GetGraphRequest> request) async {
    return getGraph(call, await request);
  }

  $async.Future<$0.GetRunErrorResponse> getRunError_Pre($grpc.ServiceCall call,
      $async.Future<$0.GetRunErrorRequest> request) async {
    return getRunError(call, await request);
  }

  $async.Future<$0.GetValidationOutputResponse> getValidationOutput_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetValidationOutputRequest> request) async {
    return getValidationOutput(call, await request);
  }

  $async.Future<$0.GetPreparationOutputResponse> getPreparationOutput_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetPreparationOutputRequest> request) async {
    return getPreparationOutput(call, await request);
  }

  $async.Future<$0.GetCompileOutputResponse> getCompileOutput_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetCompileOutputRequest> request) async {
    return getCompileOutput(call, await request);
  }

  $async.Future<$0.CancelResponse> cancel_Pre(
      $grpc.ServiceCall call, $async.Future<$0.CancelRequest> request) async {
    return cancel(call, await request);
  }

  $async.Future<$0.GetPrecompiledObjectsResponse> getPrecompiledObjects_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetPrecompiledObjectsRequest> request) async {
    return getPrecompiledObjects(call, await request);
  }

  $async.Future<$0.GetPrecompiledObjectResponse> getPrecompiledObject_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetPrecompiledObjectRequest> request) async {
    return getPrecompiledObject(call, await request);
  }

  $async.Future<$0.GetPrecompiledObjectCodeResponse>
      getPrecompiledObjectCode_Pre($grpc.ServiceCall call,
          $async.Future<$0.GetPrecompiledObjectCodeRequest> request) async {
    return getPrecompiledObjectCode(call, await request);
  }

  $async.Future<$0.GetPrecompiledObjectOutputResponse>
      getPrecompiledObjectOutput_Pre($grpc.ServiceCall call,
          $async.Future<$0.GetPrecompiledObjectOutputRequest> request) async {
    return getPrecompiledObjectOutput(call, await request);
  }

  $async.Future<$0.GetPrecompiledObjectLogsResponse>
      getPrecompiledObjectLogs_Pre($grpc.ServiceCall call,
          $async.Future<$0.GetPrecompiledObjectLogsRequest> request) async {
    return getPrecompiledObjectLogs(call, await request);
  }

  $async.Future<$0.GetPrecompiledObjectGraphResponse>
      getPrecompiledObjectGraph_Pre($grpc.ServiceCall call,
          $async.Future<$0.GetPrecompiledObjectGraphRequest> request) async {
    return getPrecompiledObjectGraph(call, await request);
  }

  $async.Future<$0.GetDefaultPrecompiledObjectResponse>
      getDefaultPrecompiledObject_Pre($grpc.ServiceCall call,
          $async.Future<$0.GetDefaultPrecompiledObjectRequest> request) async {
    return getDefaultPrecompiledObject(call, await request);
  }

  $async.Future<$0.SaveSnippetResponse> saveSnippet_Pre($grpc.ServiceCall call,
      $async.Future<$0.SaveSnippetRequest> request) async {
    return saveSnippet(call, await request);
  }

  $async.Future<$0.GetSnippetResponse> getSnippet_Pre($grpc.ServiceCall call,
      $async.Future<$0.GetSnippetRequest> request) async {
    return getSnippet(call, await request);
  }

  $async.Future<$0.GetMetadataResponse> getMetadata_Pre($grpc.ServiceCall call,
      $async.Future<$0.GetMetadataRequest> request) async {
    return getMetadata(call, await request);
  }

  $async.Future<$0.RunCodeResponse> runCode(
      $grpc.ServiceCall call, $0.RunCodeRequest request);
  $async.Future<$0.CheckStatusResponse> checkStatus(
      $grpc.ServiceCall call, $0.CheckStatusRequest request);
  $async.Future<$0.GetRunOutputResponse> getRunOutput(
      $grpc.ServiceCall call, $0.GetRunOutputRequest request);
  $async.Future<$0.GetLogsResponse> getLogs(
      $grpc.ServiceCall call, $0.GetLogsRequest request);
  $async.Future<$0.GetGraphResponse> getGraph(
      $grpc.ServiceCall call, $0.GetGraphRequest request);
  $async.Future<$0.GetRunErrorResponse> getRunError(
      $grpc.ServiceCall call, $0.GetRunErrorRequest request);
  $async.Future<$0.GetValidationOutputResponse> getValidationOutput(
      $grpc.ServiceCall call, $0.GetValidationOutputRequest request);
  $async.Future<$0.GetPreparationOutputResponse> getPreparationOutput(
      $grpc.ServiceCall call, $0.GetPreparationOutputRequest request);
  $async.Future<$0.GetCompileOutputResponse> getCompileOutput(
      $grpc.ServiceCall call, $0.GetCompileOutputRequest request);
  $async.Future<$0.CancelResponse> cancel(
      $grpc.ServiceCall call, $0.CancelRequest request);
  $async.Future<$0.GetPrecompiledObjectsResponse> getPrecompiledObjects(
      $grpc.ServiceCall call, $0.GetPrecompiledObjectsRequest request);
  $async.Future<$0.GetPrecompiledObjectResponse> getPrecompiledObject(
      $grpc.ServiceCall call, $0.GetPrecompiledObjectRequest request);
  $async.Future<$0.GetPrecompiledObjectCodeResponse> getPrecompiledObjectCode(
      $grpc.ServiceCall call, $0.GetPrecompiledObjectCodeRequest request);
  $async.Future<$0.GetPrecompiledObjectOutputResponse>
      getPrecompiledObjectOutput(
          $grpc.ServiceCall call, $0.GetPrecompiledObjectOutputRequest request);
  $async.Future<$0.GetPrecompiledObjectLogsResponse> getPrecompiledObjectLogs(
      $grpc.ServiceCall call, $0.GetPrecompiledObjectLogsRequest request);
  $async.Future<$0.GetPrecompiledObjectGraphResponse> getPrecompiledObjectGraph(
      $grpc.ServiceCall call, $0.GetPrecompiledObjectGraphRequest request);
  $async.Future<$0.GetDefaultPrecompiledObjectResponse>
      getDefaultPrecompiledObject($grpc.ServiceCall call,
          $0.GetDefaultPrecompiledObjectRequest request);
  $async.Future<$0.SaveSnippetResponse> saveSnippet(
      $grpc.ServiceCall call, $0.SaveSnippetRequest request);
  $async.Future<$0.GetSnippetResponse> getSnippet(
      $grpc.ServiceCall call, $0.GetSnippetRequest request);
  $async.Future<$0.GetMetadataResponse> getMetadata(
      $grpc.ServiceCall call, $0.GetMetadataRequest request);
}
