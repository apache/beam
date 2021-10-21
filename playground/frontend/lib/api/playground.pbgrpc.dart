///
//  Generated code. Do not modify.
//  source: playground.proto
//
// @dart = 2.12
// ignore_for_file: annotate_overrides,camel_case_types,unnecessary_const,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name,return_of_invalid_type,unnecessary_this,prefer_final_fields

import 'dart:async' as $async;

import 'dart:core' as $core;

import 'package:grpc/service_api.dart' as $grpc;
import 'playground.pb.dart' as $0;
export 'playground.pb.dart';

class PlaygroundServiceClient extends $grpc.Client {
  static final _$runCode =
      $grpc.ClientMethod<$0.RunCodeRequest, $0.RunCodeResponse>(
          '/playground.v1.PlaygroundService/RunCode',
          ($0.RunCodeRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.RunCodeResponse.fromBuffer(value));
  static final _$checkStatus =
      $grpc.ClientMethod<$0.CheckStatusRequest, $0.CheckStatusResponse>(
          '/playground.v1.PlaygroundService/CheckStatus',
          ($0.CheckStatusRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.CheckStatusResponse.fromBuffer(value));
  static final _$getRunOutput =
      $grpc.ClientMethod<$0.GetRunOutputRequest, $0.GetRunOutputResponse>(
          '/playground.v1.PlaygroundService/GetRunOutput',
          ($0.GetRunOutputRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetRunOutputResponse.fromBuffer(value));
  static final _$getCompileOutput = $grpc.ClientMethod<
          $0.GetCompileOutputRequest, $0.GetCompileOutputResponse>(
      '/playground.v1.PlaygroundService/GetCompileOutput',
      ($0.GetCompileOutputRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetCompileOutputResponse.fromBuffer(value));

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

  $grpc.ResponseFuture<$0.GetCompileOutputResponse> getCompileOutput(
      $0.GetCompileOutputRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getCompileOutput, request, options: options);
  }
}

abstract class PlaygroundServiceBase extends $grpc.Service {
  $core.String get $name => 'playground.v1.PlaygroundService';

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
    $addMethod($grpc.ServiceMethod<$0.GetCompileOutputRequest,
            $0.GetCompileOutputResponse>(
        'GetCompileOutput',
        getCompileOutput_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetCompileOutputRequest.fromBuffer(value),
        ($0.GetCompileOutputResponse value) => value.writeToBuffer()));
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

  $async.Future<$0.GetCompileOutputResponse> getCompileOutput_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetCompileOutputRequest> request) async {
    return getCompileOutput(call, await request);
  }

  $async.Future<$0.RunCodeResponse> runCode(
      $grpc.ServiceCall call, $0.RunCodeRequest request);
  $async.Future<$0.CheckStatusResponse> checkStatus(
      $grpc.ServiceCall call, $0.CheckStatusRequest request);
  $async.Future<$0.GetRunOutputResponse> getRunOutput(
      $grpc.ServiceCall call, $0.GetRunOutputRequest request);
  $async.Future<$0.GetCompileOutputResponse> getCompileOutput(
      $grpc.ServiceCall call, $0.GetCompileOutputRequest request);
}
