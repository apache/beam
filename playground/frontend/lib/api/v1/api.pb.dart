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
//  Generated code. Do not modify.
//  source: api/v1/api.proto
//
// @dart = 2.12
// ignore_for_file: annotate_overrides,camel_case_types,unnecessary_const,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name,return_of_invalid_type,unnecessary_this,prefer_final_fields

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

import 'api.pbenum.dart';

export 'api.pbenum.dart';

class RunCodeRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'RunCodeRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'code')
    ..e<Sdk>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..hasRequiredFields = false
  ;

  RunCodeRequest._() : super();
  factory RunCodeRequest({
    $core.String? code,
    Sdk? sdk,
  }) {
    final _result = create();
    if (code != null) {
      _result.code = code;
    }
    if (sdk != null) {
      _result.sdk = sdk;
    }
    return _result;
  }
  factory RunCodeRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory RunCodeRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  RunCodeRequest clone() => RunCodeRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  RunCodeRequest copyWith(void Function(RunCodeRequest) updates) => super.copyWith((message) => updates(message as RunCodeRequest)) as RunCodeRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static RunCodeRequest create() => RunCodeRequest._();
  RunCodeRequest createEmptyInstance() => create();
  static $pb.PbList<RunCodeRequest> createRepeated() => $pb.PbList<RunCodeRequest>();
  @$core.pragma('dart2js:noInline')
  static RunCodeRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RunCodeRequest>(create);
  static RunCodeRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get code => $_getSZ(0);
  @$pb.TagNumber(1)
  set code($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCode() => $_has(0);
  @$pb.TagNumber(1)
  void clearCode() => clearField(1);

  @$pb.TagNumber(2)
  Sdk get sdk => $_getN(1);
  @$pb.TagNumber(2)
  set sdk(Sdk v) { setField(2, v); }
  @$pb.TagNumber(2)
  $core.bool hasSdk() => $_has(1);
  @$pb.TagNumber(2)
  void clearSdk() => clearField(2);
}

class RunCodeResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'RunCodeResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  RunCodeResponse._() : super();
  factory RunCodeResponse({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory RunCodeResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory RunCodeResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  RunCodeResponse clone() => RunCodeResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  RunCodeResponse copyWith(void Function(RunCodeResponse) updates) => super.copyWith((message) => updates(message as RunCodeResponse)) as RunCodeResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static RunCodeResponse create() => RunCodeResponse._();
  RunCodeResponse createEmptyInstance() => create();
  static $pb.PbList<RunCodeResponse> createRepeated() => $pb.PbList<RunCodeResponse>();
  @$core.pragma('dart2js:noInline')
  static RunCodeResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RunCodeResponse>(create);
  static RunCodeResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class CheckStatusRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'CheckStatusRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  CheckStatusRequest._() : super();
  factory CheckStatusRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory CheckStatusRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory CheckStatusRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  CheckStatusRequest clone() => CheckStatusRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  CheckStatusRequest copyWith(void Function(CheckStatusRequest) updates) => super.copyWith((message) => updates(message as CheckStatusRequest)) as CheckStatusRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static CheckStatusRequest create() => CheckStatusRequest._();
  CheckStatusRequest createEmptyInstance() => create();
  static $pb.PbList<CheckStatusRequest> createRepeated() => $pb.PbList<CheckStatusRequest>();
  @$core.pragma('dart2js:noInline')
  static CheckStatusRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CheckStatusRequest>(create);
  static CheckStatusRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class CheckStatusResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'CheckStatusResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..e<Status>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'status', $pb.PbFieldType.OE, defaultOrMaker: Status.STATUS_UNSPECIFIED, valueOf: Status.valueOf, enumValues: Status.values)
    ..hasRequiredFields = false
  ;

  CheckStatusResponse._() : super();
  factory CheckStatusResponse({
    Status? status,
  }) {
    final _result = create();
    if (status != null) {
      _result.status = status;
    }
    return _result;
  }
  factory CheckStatusResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory CheckStatusResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  CheckStatusResponse clone() => CheckStatusResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  CheckStatusResponse copyWith(void Function(CheckStatusResponse) updates) => super.copyWith((message) => updates(message as CheckStatusResponse)) as CheckStatusResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static CheckStatusResponse create() => CheckStatusResponse._();
  CheckStatusResponse createEmptyInstance() => create();
  static $pb.PbList<CheckStatusResponse> createRepeated() => $pb.PbList<CheckStatusResponse>();
  @$core.pragma('dart2js:noInline')
  static CheckStatusResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CheckStatusResponse>(create);
  static CheckStatusResponse? _defaultInstance;

  @$pb.TagNumber(1)
  Status get status => $_getN(0);
  @$pb.TagNumber(1)
  set status(Status v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => clearField(1);
}

class GetCompileOutputRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetCompileOutputRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetCompileOutputRequest._() : super();
  factory GetCompileOutputRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory GetCompileOutputRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetCompileOutputRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetCompileOutputRequest clone() => GetCompileOutputRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetCompileOutputRequest copyWith(void Function(GetCompileOutputRequest) updates) => super.copyWith((message) => updates(message as GetCompileOutputRequest)) as GetCompileOutputRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetCompileOutputRequest create() => GetCompileOutputRequest._();
  GetCompileOutputRequest createEmptyInstance() => create();
  static $pb.PbList<GetCompileOutputRequest> createRepeated() => $pb.PbList<GetCompileOutputRequest>();
  @$core.pragma('dart2js:noInline')
  static GetCompileOutputRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetCompileOutputRequest>(create);
  static GetCompileOutputRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetCompileOutputResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetCompileOutputResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..e<Status>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'compilationStatus', $pb.PbFieldType.OE, defaultOrMaker: Status.STATUS_UNSPECIFIED, valueOf: Status.valueOf, enumValues: Status.values)
    ..hasRequiredFields = false
  ;

  GetCompileOutputResponse._() : super();
  factory GetCompileOutputResponse({
    $core.String? output,
    Status? compilationStatus,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    if (compilationStatus != null) {
      _result.compilationStatus = compilationStatus;
    }
    return _result;
  }
  factory GetCompileOutputResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetCompileOutputResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetCompileOutputResponse clone() => GetCompileOutputResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetCompileOutputResponse copyWith(void Function(GetCompileOutputResponse) updates) => super.copyWith((message) => updates(message as GetCompileOutputResponse)) as GetCompileOutputResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetCompileOutputResponse create() => GetCompileOutputResponse._();
  GetCompileOutputResponse createEmptyInstance() => create();
  static $pb.PbList<GetCompileOutputResponse> createRepeated() => $pb.PbList<GetCompileOutputResponse>();
  @$core.pragma('dart2js:noInline')
  static GetCompileOutputResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetCompileOutputResponse>(create);
  static GetCompileOutputResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);

  @$pb.TagNumber(2)
  Status get compilationStatus => $_getN(1);
  @$pb.TagNumber(2)
  set compilationStatus(Status v) { setField(2, v); }
  @$pb.TagNumber(2)
  $core.bool hasCompilationStatus() => $_has(1);
  @$pb.TagNumber(2)
  void clearCompilationStatus() => clearField(2);
}

class GetRunOutputRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetRunOutputRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetRunOutputRequest._() : super();
  factory GetRunOutputRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory GetRunOutputRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetRunOutputRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetRunOutputRequest clone() => GetRunOutputRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetRunOutputRequest copyWith(void Function(GetRunOutputRequest) updates) => super.copyWith((message) => updates(message as GetRunOutputRequest)) as GetRunOutputRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetRunOutputRequest create() => GetRunOutputRequest._();
  GetRunOutputRequest createEmptyInstance() => create();
  static $pb.PbList<GetRunOutputRequest> createRepeated() => $pb.PbList<GetRunOutputRequest>();
  @$core.pragma('dart2js:noInline')
  static GetRunOutputRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetRunOutputRequest>(create);
  static GetRunOutputRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetRunOutputResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetRunOutputResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..hasRequiredFields = false
  ;

  GetRunOutputResponse._() : super();
  factory GetRunOutputResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }
  factory GetRunOutputResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetRunOutputResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetRunOutputResponse clone() => GetRunOutputResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetRunOutputResponse copyWith(void Function(GetRunOutputResponse) updates) => super.copyWith((message) => updates(message as GetRunOutputResponse)) as GetRunOutputResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetRunOutputResponse create() => GetRunOutputResponse._();
  GetRunOutputResponse createEmptyInstance() => create();
  static $pb.PbList<GetRunOutputResponse> createRepeated() => $pb.PbList<GetRunOutputResponse>();
  @$core.pragma('dart2js:noInline')
  static GetRunOutputResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetRunOutputResponse>(create);
  static GetRunOutputResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetRunErrorRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetRunErrorRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetRunErrorRequest._() : super();
  factory GetRunErrorRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory GetRunErrorRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetRunErrorRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetRunErrorRequest clone() => GetRunErrorRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetRunErrorRequest copyWith(void Function(GetRunErrorRequest) updates) => super.copyWith((message) => updates(message as GetRunErrorRequest)) as GetRunErrorRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetRunErrorRequest create() => GetRunErrorRequest._();
  GetRunErrorRequest createEmptyInstance() => create();
  static $pb.PbList<GetRunErrorRequest> createRepeated() => $pb.PbList<GetRunErrorRequest>();
  @$core.pragma('dart2js:noInline')
  static GetRunErrorRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetRunErrorRequest>(create);
  static GetRunErrorRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetRunErrorResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetRunErrorResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..hasRequiredFields = false
  ;

  GetRunErrorResponse._() : super();

  factory GetRunErrorResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }

  factory GetRunErrorResponse.fromBuffer($core.List<$core.int> i,
      [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) =>
      create()
        ..mergeFromBuffer(i, r);

  factory GetRunErrorResponse.fromJson($core.String i,
      [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) =>
      create()
        ..mergeFromJson(i, r);

  @$core.Deprecated(
      'Using this can add significant overhead to your binary. '
          'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
          'Will be removed in next major version')
  GetRunErrorResponse clone() =>
      GetRunErrorResponse()
        ..mergeFromMessage(this);

  @$core.Deprecated(
      'Using this can add significant overhead to your binary. '
          'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
          'Will be removed in next major version')
  GetRunErrorResponse copyWith(void Function(GetRunErrorResponse) updates) =>
      super.copyWith((message) => updates(
          message as GetRunErrorResponse)) as GetRunErrorResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static GetRunErrorResponse create() => GetRunErrorResponse._();

  GetRunErrorResponse createEmptyInstance() => create();

  static $pb.PbList<GetRunErrorResponse> createRepeated() =>
      $pb.PbList<GetRunErrorResponse>();

  @$core.pragma('dart2js:noInline')
  static GetRunErrorResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GetRunErrorResponse>(create);
  static GetRunErrorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);

  @$pb.TagNumber(1)
  set output($core.String v) {
    $_setString(0, v);
  }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetLogsRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      const $core.bool.fromEnvironment('protobuf.omit_message_names')
          ? ''
          : 'GetLogsRequest', package: const $pb.PackageName(
      const $core.bool.fromEnvironment('protobuf.omit_message_names')
          ? ''
          : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names')
        ? ''
        : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetLogsRequest._() : super();

  factory GetLogsRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }

  factory GetLogsRequest.fromBuffer($core.List<$core.int> i,
      [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) =>
      create()
        ..mergeFromBuffer(i, r);

  factory GetLogsRequest.fromJson($core.String i,
      [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) =>
      create()
        ..mergeFromJson(i, r);

  @$core.Deprecated(
      'Using this can add significant overhead to your binary. '
          'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
          'Will be removed in next major version')
  GetLogsRequest clone() =>
      GetLogsRequest()
        ..mergeFromMessage(this);

  @$core.Deprecated(
      'Using this can add significant overhead to your binary. '
          'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
          'Will be removed in next major version')
  GetLogsRequest copyWith(void Function(GetLogsRequest) updates) =>
      super.copyWith((message) => updates(
          message as GetLogsRequest)) as GetLogsRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static GetLogsRequest create() => GetLogsRequest._();

  GetLogsRequest createEmptyInstance() => create();

  static $pb.PbList<GetLogsRequest> createRepeated() =>
      $pb.PbList<GetLogsRequest>();

  @$core.pragma('dart2js:noInline')
  static GetLogsRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GetLogsRequest>(create);
  static GetLogsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);

  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) {
    $_setString(0, v);
  }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetLogsResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      const $core.bool.fromEnvironment('protobuf.omit_message_names')
          ? ''
          : 'GetLogsResponse', package: const $pb.PackageName(
      const $core.bool.fromEnvironment('protobuf.omit_message_names')
          ? ''
          : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names')
        ? ''
        : 'output')
    ..hasRequiredFields = false
  ;

  GetLogsResponse._() : super();

  factory GetLogsResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }

  factory GetLogsResponse.fromBuffer($core.List<$core.int> i,
      [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) =>
      create()
        ..mergeFromBuffer(i, r);

  factory GetLogsResponse.fromJson($core.String i,
      [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) =>
      create()
        ..mergeFromJson(i, r);

  @$core.Deprecated(
      'Using this can add significant overhead to your binary. '
          'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
          'Will be removed in next major version')
  GetLogsResponse clone() =>
      GetLogsResponse()
        ..mergeFromMessage(this);

  @$core.Deprecated(
      'Using this can add significant overhead to your binary. '
          'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
          'Will be removed in next major version')
  GetLogsResponse copyWith(void Function(GetLogsResponse) updates) =>
      super.copyWith((message) => updates(
          message as GetLogsResponse)) as GetLogsResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static GetLogsResponse create() => GetLogsResponse._();

  GetLogsResponse createEmptyInstance() => create();

  static $pb.PbList<GetLogsResponse> createRepeated() =>
      $pb.PbList<GetLogsResponse>();

  @$core.pragma('dart2js:noInline')
  static GetLogsResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GetLogsResponse>(create);
  static GetLogsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);

  @$pb.TagNumber(1)
  set output($core.String v) {
    $_setString(0, v);
  }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class CancelRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      const $core.bool.fromEnvironment('protobuf.omit_message_names')
          ? ''
          : 'CancelRequest', package: const $pb.PackageName(
      const $core.bool.fromEnvironment('protobuf.omit_message_names')
          ? ''
          : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names')
        ? ''
        : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  CancelRequest._() : super();

  factory CancelRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory CancelRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory CancelRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  CancelRequest clone() => CancelRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  CancelRequest copyWith(void Function(CancelRequest) updates) => super.copyWith((message) => updates(message as CancelRequest)) as CancelRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static CancelRequest create() => CancelRequest._();
  CancelRequest createEmptyInstance() => create();
  static $pb.PbList<CancelRequest> createRepeated() => $pb.PbList<CancelRequest>();
  @$core.pragma('dart2js:noInline')
  static CancelRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CancelRequest>(create);
  static CancelRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class CancelResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'CancelResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..hasRequiredFields = false
  ;

  CancelResponse._() : super();
  factory CancelResponse() => create();
  factory CancelResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory CancelResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  CancelResponse clone() => CancelResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  CancelResponse copyWith(void Function(CancelResponse) updates) => super.copyWith((message) => updates(message as CancelResponse)) as CancelResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static CancelResponse create() => CancelResponse._();
  CancelResponse createEmptyInstance() => create();
  static $pb.PbList<CancelResponse> createRepeated() => $pb.PbList<CancelResponse>();
  @$core.pragma('dart2js:noInline')
  static CancelResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CancelResponse>(create);
  static CancelResponse? _defaultInstance;
}

class GetPrecompiledObjectsRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectsRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..e<Sdk>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..aOS(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'category')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectsRequest._() : super();
  factory GetPrecompiledObjectsRequest({
    Sdk? sdk,
    $core.String? category,
  }) {
    final _result = create();
    if (sdk != null) {
      _result.sdk = sdk;
    }
    if (category != null) {
      _result.category = category;
    }
    return _result;
  }
  factory GetPrecompiledObjectsRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectsRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectsRequest clone() => GetPrecompiledObjectsRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectsRequest copyWith(void Function(GetPrecompiledObjectsRequest) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectsRequest)) as GetPrecompiledObjectsRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectsRequest create() => GetPrecompiledObjectsRequest._();
  GetPrecompiledObjectsRequest createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectsRequest> createRepeated() => $pb.PbList<GetPrecompiledObjectsRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectsRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectsRequest>(create);
  static GetPrecompiledObjectsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  Sdk get sdk => $_getN(0);
  @$pb.TagNumber(1)
  set sdk(Sdk v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasSdk() => $_has(0);
  @$pb.TagNumber(1)
  void clearSdk() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get category => $_getSZ(1);
  @$pb.TagNumber(2)
  set category($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasCategory() => $_has(1);
  @$pb.TagNumber(2)
  void clearCategory() => clearField(2);
}

class PrecompiledObject extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'PrecompiledObject', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..aOS(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'name')
    ..aOS(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'description')
    ..e<PrecompiledObjectType>(4,
        const $core.bool.fromEnvironment('protobuf.omit_field_names')
            ? ''
            : 'type', $pb.PbFieldType.OE, defaultOrMaker: PrecompiledObjectType
            .PRECOMPILED_OBJECT_TYPE_UNSPECIFIED,
        valueOf: PrecompiledObjectType.valueOf,
        enumValues: PrecompiledObjectType.values)
    ..aOS(5, const $core.bool.fromEnvironment('protobuf.omit_field_names')
        ? ''
        : 'pipelineOptions')
    ..hasRequiredFields = false
  ;

  PrecompiledObject._() : super();
  factory PrecompiledObject({
    $core.String? cloudPath,
    $core.String? name,
    $core.String? description,
    PrecompiledObjectType? type,
    $core.String? pipelineOptions,
  }) {
    final _result = create();
    if (cloudPath != null) {
      _result.cloudPath = cloudPath;
    }
    if (name != null) {
      _result.name = name;
    }
    if (description != null) {
      _result.description = description;
    }
    if (type != null) {
      _result.type = type;
    }
    if (pipelineOptions != null) {
      _result.pipelineOptions = pipelineOptions;
    }
    return _result;
  }
  factory PrecompiledObject.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory PrecompiledObject.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  PrecompiledObject clone() => PrecompiledObject()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  PrecompiledObject copyWith(void Function(PrecompiledObject) updates) => super.copyWith((message) => updates(message as PrecompiledObject)) as PrecompiledObject; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static PrecompiledObject create() => PrecompiledObject._();
  PrecompiledObject createEmptyInstance() => create();
  static $pb.PbList<PrecompiledObject> createRepeated() => $pb.PbList<PrecompiledObject>();
  @$core.pragma('dart2js:noInline')
  static PrecompiledObject getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<PrecompiledObject>(create);
  static PrecompiledObject? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cloudPath => $_getSZ(0);
  @$pb.TagNumber(1)
  set cloudPath($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCloudPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearCloudPath() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => clearField(2);

  @$pb.TagNumber(3)
  $core.String get description => $_getSZ(2);
  @$pb.TagNumber(3)
  set description($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasDescription() => $_has(2);
  @$pb.TagNumber(3)
  void clearDescription() => clearField(3);

  @$pb.TagNumber(4)
  PrecompiledObjectType get type => $_getN(3);

  @$pb.TagNumber(4)
  set type(PrecompiledObjectType v) {
    setField(4, v);
  }

  @$pb.TagNumber(4)
  $core.bool hasType() => $_has(3);

  @$pb.TagNumber(4)
  void clearType() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get pipelineOptions => $_getSZ(4);

  @$pb.TagNumber(5)
  set pipelineOptions($core.String v) {
    $_setString(4, v);
  }

  @$pb.TagNumber(5)
  $core.bool hasPipelineOptions() => $_has(4);

  @$pb.TagNumber(5)
  void clearPipelineOptions() => clearField(5);
}

class Categories_Category extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'Categories.Category', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'categoryName')
    ..pc<PrecompiledObject>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'precompiledObjects', $pb.PbFieldType.PM, subBuilder: PrecompiledObject.create)
    ..hasRequiredFields = false
  ;

  Categories_Category._() : super();
  factory Categories_Category({
    $core.String? categoryName,
    $core.Iterable<PrecompiledObject>? precompiledObjects,
  }) {
    final _result = create();
    if (categoryName != null) {
      _result.categoryName = categoryName;
    }
    if (precompiledObjects != null) {
      _result.precompiledObjects.addAll(precompiledObjects);
    }
    return _result;
  }
  factory Categories_Category.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Categories_Category.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Categories_Category clone() => Categories_Category()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Categories_Category copyWith(void Function(Categories_Category) updates) => super.copyWith((message) => updates(message as Categories_Category)) as Categories_Category; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static Categories_Category create() => Categories_Category._();
  Categories_Category createEmptyInstance() => create();
  static $pb.PbList<Categories_Category> createRepeated() => $pb.PbList<Categories_Category>();
  @$core.pragma('dart2js:noInline')
  static Categories_Category getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Categories_Category>(create);
  static Categories_Category? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get categoryName => $_getSZ(0);
  @$pb.TagNumber(1)
  set categoryName($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCategoryName() => $_has(0);
  @$pb.TagNumber(1)
  void clearCategoryName() => clearField(1);

  @$pb.TagNumber(2)
  $core.List<PrecompiledObject> get precompiledObjects => $_getList(1);
}

class Categories extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'Categories', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..e<Sdk>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..pc<Categories_Category>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'categories', $pb.PbFieldType.PM, subBuilder: Categories_Category.create)
    ..hasRequiredFields = false
  ;

  Categories._() : super();
  factory Categories({
    Sdk? sdk,
    $core.Iterable<Categories_Category>? categories,
  }) {
    final _result = create();
    if (sdk != null) {
      _result.sdk = sdk;
    }
    if (categories != null) {
      _result.categories.addAll(categories);
    }
    return _result;
  }
  factory Categories.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Categories.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Categories clone() => Categories()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Categories copyWith(void Function(Categories) updates) => super.copyWith((message) => updates(message as Categories)) as Categories; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static Categories create() => Categories._();
  Categories createEmptyInstance() => create();
  static $pb.PbList<Categories> createRepeated() => $pb.PbList<Categories>();
  @$core.pragma('dart2js:noInline')
  static Categories getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Categories>(create);
  static Categories? _defaultInstance;

  @$pb.TagNumber(1)
  Sdk get sdk => $_getN(0);
  @$pb.TagNumber(1)
  set sdk(Sdk v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasSdk() => $_has(0);
  @$pb.TagNumber(1)
  void clearSdk() => clearField(1);

  @$pb.TagNumber(2)
  $core.List<Categories_Category> get categories => $_getList(1);
}

class GetPrecompiledObjectsResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectsResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..pc<Categories>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdkCategories', $pb.PbFieldType.PM, subBuilder: Categories.create)
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectsResponse._() : super();
  factory GetPrecompiledObjectsResponse({
    $core.Iterable<Categories>? sdkCategories,
  }) {
    final _result = create();
    if (sdkCategories != null) {
      _result.sdkCategories.addAll(sdkCategories);
    }
    return _result;
  }
  factory GetPrecompiledObjectsResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectsResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectsResponse clone() => GetPrecompiledObjectsResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectsResponse copyWith(void Function(GetPrecompiledObjectsResponse) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectsResponse)) as GetPrecompiledObjectsResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectsResponse create() => GetPrecompiledObjectsResponse._();
  GetPrecompiledObjectsResponse createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectsResponse> createRepeated() => $pb.PbList<GetPrecompiledObjectsResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectsResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectsResponse>(create);
  static GetPrecompiledObjectsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<Categories> get sdkCategories => $_getList(0);
}

class GetPrecompiledObjectRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectRequest._() : super();
  factory GetPrecompiledObjectRequest({
    $core.String? cloudPath,
  }) {
    final _result = create();
    if (cloudPath != null) {
      _result.cloudPath = cloudPath;
    }
    return _result;
  }
  factory GetPrecompiledObjectRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectRequest clone() => GetPrecompiledObjectRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectRequest copyWith(void Function(GetPrecompiledObjectRequest) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectRequest)) as GetPrecompiledObjectRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectRequest create() => GetPrecompiledObjectRequest._();
  GetPrecompiledObjectRequest createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectRequest> createRepeated() => $pb.PbList<GetPrecompiledObjectRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectRequest>(create);
  static GetPrecompiledObjectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cloudPath => $_getSZ(0);
  @$pb.TagNumber(1)
  set cloudPath($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCloudPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearCloudPath() => clearField(1);
}

class GetPrecompiledObjectCodeResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectCodeResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'code')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectCodeResponse._() : super();
  factory GetPrecompiledObjectCodeResponse({
    $core.String? code,
  }) {
    final _result = create();
    if (code != null) {
      _result.code = code;
    }
    return _result;
  }
  factory GetPrecompiledObjectCodeResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectCodeResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectCodeResponse clone() => GetPrecompiledObjectCodeResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectCodeResponse copyWith(void Function(GetPrecompiledObjectCodeResponse) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectCodeResponse)) as GetPrecompiledObjectCodeResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectCodeResponse create() => GetPrecompiledObjectCodeResponse._();
  GetPrecompiledObjectCodeResponse createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectCodeResponse> createRepeated() => $pb.PbList<GetPrecompiledObjectCodeResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectCodeResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectCodeResponse>(create);
  static GetPrecompiledObjectCodeResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get code => $_getSZ(0);
  @$pb.TagNumber(1)
  set code($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCode() => $_has(0);
  @$pb.TagNumber(1)
  void clearCode() => clearField(1);
}

