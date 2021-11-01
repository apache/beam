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
    ..e<Status>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'compilationStatus', $pb.PbFieldType.OE, defaultOrMaker: Status.STATUS_UNSPECIFIED, valueOf: Status.valueOf, enumValues: Status.values)
    ..hasRequiredFields = false
  ;

  GetRunOutputResponse._() : super();
  factory GetRunOutputResponse({
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

  @$pb.TagNumber(2)
  Status get compilationStatus => $_getN(1);
  @$pb.TagNumber(2)
  set compilationStatus(Status v) { setField(2, v); }
  @$pb.TagNumber(2)
  $core.bool hasCompilationStatus() => $_has(1);
  @$pb.TagNumber(2)
  void clearCompilationStatus() => clearField(2);
}

class GetListOfExamplesRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetListOfExamplesRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..e<Sdk>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..aOS(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'category')
    ..hasRequiredFields = false
  ;

  GetListOfExamplesRequest._() : super();
  factory GetListOfExamplesRequest({
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
  factory GetListOfExamplesRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetListOfExamplesRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetListOfExamplesRequest clone() => GetListOfExamplesRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetListOfExamplesRequest copyWith(void Function(GetListOfExamplesRequest) updates) => super.copyWith((message) => updates(message as GetListOfExamplesRequest)) as GetListOfExamplesRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetListOfExamplesRequest create() => GetListOfExamplesRequest._();
  GetListOfExamplesRequest createEmptyInstance() => create();
  static $pb.PbList<GetListOfExamplesRequest> createRepeated() => $pb.PbList<GetListOfExamplesRequest>();
  @$core.pragma('dart2js:noInline')
  static GetListOfExamplesRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetListOfExamplesRequest>(create);
  static GetListOfExamplesRequest? _defaultInstance;

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

class Examples extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'Examples', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..pPS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'example')
    ..hasRequiredFields = false
  ;

  Examples._() : super();
  factory Examples({
    $core.Iterable<$core.String>? example,
  }) {
    final _result = create();
    if (example != null) {
      _result.example.addAll(example);
    }
    return _result;
  }
  factory Examples.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Examples.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Examples clone() => Examples()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Examples copyWith(void Function(Examples) updates) => super.copyWith((message) => updates(message as Examples)) as Examples; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static Examples create() => Examples._();
  Examples createEmptyInstance() => create();
  static $pb.PbList<Examples> createRepeated() => $pb.PbList<Examples>();
  @$core.pragma('dart2js:noInline')
  static Examples getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Examples>(create);
  static Examples? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.String> get example => $_getList(0);
}

class CategoryList extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'CategoryList', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..m<$core.String, Examples>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'categoryExamples', entryClassName: 'CategoryList.CategoryExamplesEntry', keyFieldType: $pb.PbFieldType.OS, valueFieldType: $pb.PbFieldType.OM, valueCreator: Examples.create, packageName: const $pb.PackageName('api.v1'))
    ..hasRequiredFields = false
  ;

  CategoryList._() : super();
  factory CategoryList({
    $core.Map<$core.String, Examples>? categoryExamples,
  }) {
    final _result = create();
    if (categoryExamples != null) {
      _result.categoryExamples.addAll(categoryExamples);
    }
    return _result;
  }
  factory CategoryList.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory CategoryList.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  CategoryList clone() => CategoryList()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  CategoryList copyWith(void Function(CategoryList) updates) => super.copyWith((message) => updates(message as CategoryList)) as CategoryList; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static CategoryList create() => CategoryList._();
  CategoryList createEmptyInstance() => create();
  static $pb.PbList<CategoryList> createRepeated() => $pb.PbList<CategoryList>();
  @$core.pragma('dart2js:noInline')
  static CategoryList getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CategoryList>(create);
  static CategoryList? _defaultInstance;

  @$pb.TagNumber(1)
  $core.Map<$core.String, Examples> get categoryExamples => $_getMap(0);
}

class GetListOfExamplesResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetListOfExamplesResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..m<$core.String, CategoryList>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdkCategories', entryClassName: 'GetListOfExamplesResponse.SdkCategoriesEntry', keyFieldType: $pb.PbFieldType.OS, valueFieldType: $pb.PbFieldType.OM, valueCreator: CategoryList.create, packageName: const $pb.PackageName('api.v1'))
    ..hasRequiredFields = false
  ;

  GetListOfExamplesResponse._() : super();
  factory GetListOfExamplesResponse({
    $core.Map<$core.String, CategoryList>? sdkCategories,
  }) {
    final _result = create();
    if (sdkCategories != null) {
      _result.sdkCategories.addAll(sdkCategories);
    }
    return _result;
  }
  factory GetListOfExamplesResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetListOfExamplesResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetListOfExamplesResponse clone() => GetListOfExamplesResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetListOfExamplesResponse copyWith(void Function(GetListOfExamplesResponse) updates) => super.copyWith((message) => updates(message as GetListOfExamplesResponse)) as GetListOfExamplesResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetListOfExamplesResponse create() => GetListOfExamplesResponse._();
  GetListOfExamplesResponse createEmptyInstance() => create();
  static $pb.PbList<GetListOfExamplesResponse> createRepeated() => $pb.PbList<GetListOfExamplesResponse>();
  @$core.pragma('dart2js:noInline')
  static GetListOfExamplesResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetListOfExamplesResponse>(create);
  static GetListOfExamplesResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.Map<$core.String, CategoryList> get sdkCategories => $_getMap(0);
}

class GetExampleRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetExampleRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'exampleUuid')
    ..hasRequiredFields = false
  ;

  GetExampleRequest._() : super();
  factory GetExampleRequest({
    $core.String? exampleUuid,
  }) {
    final _result = create();
    if (exampleUuid != null) {
      _result.exampleUuid = exampleUuid;
    }
    return _result;
  }
  factory GetExampleRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetExampleRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetExampleRequest clone() => GetExampleRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetExampleRequest copyWith(void Function(GetExampleRequest) updates) => super.copyWith((message) => updates(message as GetExampleRequest)) as GetExampleRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetExampleRequest create() => GetExampleRequest._();
  GetExampleRequest createEmptyInstance() => create();
  static $pb.PbList<GetExampleRequest> createRepeated() => $pb.PbList<GetExampleRequest>();
  @$core.pragma('dart2js:noInline')
  static GetExampleRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetExampleRequest>(create);
  static GetExampleRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get exampleUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set exampleUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasExampleUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearExampleUuid() => clearField(1);
}

class GetExampleResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetExampleResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'code')
    ..hasRequiredFields = false
  ;

  GetExampleResponse._() : super();
  factory GetExampleResponse({
    $core.String? code,
  }) {
    final _result = create();
    if (code != null) {
      _result.code = code;
    }
    return _result;
  }
  factory GetExampleResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetExampleResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetExampleResponse clone() => GetExampleResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetExampleResponse copyWith(void Function(GetExampleResponse) updates) => super.copyWith((message) => updates(message as GetExampleResponse)) as GetExampleResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetExampleResponse create() => GetExampleResponse._();
  GetExampleResponse createEmptyInstance() => create();
  static $pb.PbList<GetExampleResponse> createRepeated() => $pb.PbList<GetExampleResponse>();
  @$core.pragma('dart2js:noInline')
  static GetExampleResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetExampleResponse>(create);
  static GetExampleResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get code => $_getSZ(0);
  @$pb.TagNumber(1)
  set code($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCode() => $_has(0);
  @$pb.TagNumber(1)
  void clearCode() => clearField(1);
}

