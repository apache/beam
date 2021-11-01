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
///
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
  static final _$getCompileOutput = $grpc.ClientMethod<
          $0.GetCompileOutputRequest, $0.GetCompileOutputResponse>(
      '/api.v1.PlaygroundService/GetCompileOutput',
      ($0.GetCompileOutputRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetCompileOutputResponse.fromBuffer(value));
  static final _$getListOfExamples = $grpc.ClientMethod<
          $0.GetListOfExamplesRequest, $0.GetListOfExamplesResponse>(
      '/api.v1.PlaygroundService/GetListOfExamples',
      ($0.GetListOfExamplesRequest value) => value.writeToBuffer(),
      ($core.List<$core.int> value) =>
          $0.GetListOfExamplesResponse.fromBuffer(value));
  static final _$getExample =
      $grpc.ClientMethod<$0.GetExampleRequest, $0.GetExampleResponse>(
          '/api.v1.PlaygroundService/GetExample',
          ($0.GetExampleRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetExampleResponse.fromBuffer(value));
  static final _$getExampleOutput =
      $grpc.ClientMethod<$0.GetExampleRequest, $0.GetRunOutputResponse>(
          '/api.v1.PlaygroundService/GetExampleOutput',
          ($0.GetExampleRequest value) => value.writeToBuffer(),
          ($core.List<$core.int> value) =>
              $0.GetRunOutputResponse.fromBuffer(value));

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

  $grpc.ResponseFuture<$0.GetListOfExamplesResponse> getListOfExamples(
      $0.GetListOfExamplesRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getListOfExamples, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetExampleResponse> getExample(
      $0.GetExampleRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getExample, request, options: options);
  }

  $grpc.ResponseFuture<$0.GetRunOutputResponse> getExampleOutput(
      $0.GetExampleRequest request,
      {$grpc.CallOptions? options}) {
    return $createUnaryCall(_$getExampleOutput, request, options: options);
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
    $addMethod($grpc.ServiceMethod<$0.GetCompileOutputRequest,
            $0.GetCompileOutputResponse>(
        'GetCompileOutput',
        getCompileOutput_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetCompileOutputRequest.fromBuffer(value),
        ($0.GetCompileOutputResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetListOfExamplesRequest,
            $0.GetListOfExamplesResponse>(
        'GetListOfExamples',
        getListOfExamples_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.GetListOfExamplesRequest.fromBuffer(value),
        ($0.GetListOfExamplesResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetExampleRequest, $0.GetExampleResponse>(
        'GetExample',
        getExample_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.GetExampleRequest.fromBuffer(value),
        ($0.GetExampleResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.GetExampleRequest, $0.GetRunOutputResponse>(
            'GetExampleOutput',
            getExampleOutput_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.GetExampleRequest.fromBuffer(value),
            ($0.GetRunOutputResponse value) => value.writeToBuffer()));
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

  $async.Future<$0.GetListOfExamplesResponse> getListOfExamples_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetListOfExamplesRequest> request) async {
    return getListOfExamples(call, await request);
  }

  $async.Future<$0.GetExampleResponse> getExample_Pre($grpc.ServiceCall call,
      $async.Future<$0.GetExampleRequest> request) async {
    return getExample(call, await request);
  }

  $async.Future<$0.GetRunOutputResponse> getExampleOutput_Pre(
      $grpc.ServiceCall call,
      $async.Future<$0.GetExampleRequest> request) async {
    return getExampleOutput(call, await request);
  }

  $async.Future<$0.RunCodeResponse> runCode(
      $grpc.ServiceCall call, $0.RunCodeRequest request);

  $async.Future<$0.CheckStatusResponse> checkStatus(
      $grpc.ServiceCall call, $0.CheckStatusRequest request);

  $async.Future<$0.GetRunOutputResponse> getRunOutput(
      $grpc.ServiceCall call, $0.GetRunOutputRequest request);

  $async.Future<$0.GetCompileOutputResponse> getCompileOutput(
      $grpc.ServiceCall call, $0.GetCompileOutputRequest request);

  $async.Future<$0.GetListOfExamplesResponse> getListOfExamples(
      $grpc.ServiceCall call, $0.GetListOfExamplesRequest request);

  $async.Future<$0.GetExampleResponse> getExample(
      $grpc.ServiceCall call, $0.GetExampleRequest request);

  $async.Future<$0.GetRunOutputResponse> getExampleOutput(
      $grpc.ServiceCall call, $0.GetExampleRequest request);
}
