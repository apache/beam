///
//  Generated code. Do not modify.
//  source: api/v1/api.proto
//
// @dart = 2.12
// ignore_for_file: annotate_overrides,camel_case_types,constant_identifier_names,directives_ordering,library_prefixes,non_constant_identifier_names,prefer_final_fields,return_of_invalid_type,unnecessary_const,unnecessary_import,unnecessary_this,unused_import,unused_shown_name

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'api.pbenum.dart';

export 'api.pbenum.dart';

class Dataset extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'Dataset', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..e<EmulatorType>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'type', $pb.PbFieldType.OE, defaultOrMaker: EmulatorType.EMULATOR_TYPE_UNSPECIFIED, valueOf: EmulatorType.valueOf, enumValues: EmulatorType.values)
    ..m<$core.String, $core.String>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'options', entryClassName: 'Dataset.OptionsEntry', keyFieldType: $pb.PbFieldType.OS, valueFieldType: $pb.PbFieldType.OS, packageName: const $pb.PackageName('api.v1'))
    ..aOS(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'datasetPath')
    ..hasRequiredFields = false
  ;

  Dataset._() : super();
  factory Dataset({
    EmulatorType? type,
    $core.Map<$core.String, $core.String>? options,
    $core.String? datasetPath,
  }) {
    final _result = create();
    if (type != null) {
      _result.type = type;
    }
    if (options != null) {
      _result.options.addAll(options);
    }
    if (datasetPath != null) {
      _result.datasetPath = datasetPath;
    }
    return _result;
  }
  factory Dataset.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Dataset.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Dataset clone() => Dataset()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Dataset copyWith(void Function(Dataset) updates) => super.copyWith((message) => updates(message as Dataset)) as Dataset; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static Dataset create() => Dataset._();
  Dataset createEmptyInstance() => create();
  static $pb.PbList<Dataset> createRepeated() => $pb.PbList<Dataset>();
  @$core.pragma('dart2js:noInline')
  static Dataset getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Dataset>(create);
  static Dataset? _defaultInstance;

  @$pb.TagNumber(1)
  EmulatorType get type => $_getN(0);
  @$pb.TagNumber(1)
  set type(EmulatorType v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasType() => $_has(0);
  @$pb.TagNumber(1)
  void clearType() => clearField(1);

  @$pb.TagNumber(2)
  $core.Map<$core.String, $core.String> get options => $_getMap(1);

  @$pb.TagNumber(3)
  $core.String get datasetPath => $_getSZ(2);
  @$pb.TagNumber(3)
  set datasetPath($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasDatasetPath() => $_has(2);
  @$pb.TagNumber(3)
  void clearDatasetPath() => clearField(3);
}

class RunCodeRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'RunCodeRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'code')
    ..e<Sdk>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..aOS(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineOptions')
    ..pc<Dataset>(4, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'datasets', $pb.PbFieldType.PM, subBuilder: Dataset.create)
    ..pc<SnippetFile>(5, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'files', $pb.PbFieldType.PM, subBuilder: SnippetFile.create)
    ..hasRequiredFields = false
  ;

  RunCodeRequest._() : super();
  factory RunCodeRequest({
    $core.String? code,
    Sdk? sdk,
    $core.String? pipelineOptions,
    $core.Iterable<Dataset>? datasets,
    $core.Iterable<SnippetFile>? files,
  }) {
    final _result = create();
    if (code != null) {
      _result.code = code;
    }
    if (sdk != null) {
      _result.sdk = sdk;
    }
    if (pipelineOptions != null) {
      _result.pipelineOptions = pipelineOptions;
    }
    if (datasets != null) {
      _result.datasets.addAll(datasets);
    }
    if (files != null) {
      _result.files.addAll(files);
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

  @$pb.TagNumber(3)
  $core.String get pipelineOptions => $_getSZ(2);
  @$pb.TagNumber(3)
  set pipelineOptions($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasPipelineOptions() => $_has(2);
  @$pb.TagNumber(3)
  void clearPipelineOptions() => clearField(3);

  @$pb.TagNumber(4)
  $core.List<Dataset> get datasets => $_getList(3);

  @$pb.TagNumber(5)
  $core.List<SnippetFile> get files => $_getList(4);
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

class GetValidationOutputRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetValidationOutputRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetValidationOutputRequest._() : super();
  factory GetValidationOutputRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory GetValidationOutputRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetValidationOutputRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetValidationOutputRequest clone() => GetValidationOutputRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetValidationOutputRequest copyWith(void Function(GetValidationOutputRequest) updates) => super.copyWith((message) => updates(message as GetValidationOutputRequest)) as GetValidationOutputRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetValidationOutputRequest create() => GetValidationOutputRequest._();
  GetValidationOutputRequest createEmptyInstance() => create();
  static $pb.PbList<GetValidationOutputRequest> createRepeated() => $pb.PbList<GetValidationOutputRequest>();
  @$core.pragma('dart2js:noInline')
  static GetValidationOutputRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetValidationOutputRequest>(create);
  static GetValidationOutputRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetValidationOutputResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetValidationOutputResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..hasRequiredFields = false
  ;

  GetValidationOutputResponse._() : super();
  factory GetValidationOutputResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }
  factory GetValidationOutputResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetValidationOutputResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetValidationOutputResponse clone() => GetValidationOutputResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetValidationOutputResponse copyWith(void Function(GetValidationOutputResponse) updates) => super.copyWith((message) => updates(message as GetValidationOutputResponse)) as GetValidationOutputResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetValidationOutputResponse create() => GetValidationOutputResponse._();
  GetValidationOutputResponse createEmptyInstance() => create();
  static $pb.PbList<GetValidationOutputResponse> createRepeated() => $pb.PbList<GetValidationOutputResponse>();
  @$core.pragma('dart2js:noInline')
  static GetValidationOutputResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetValidationOutputResponse>(create);
  static GetValidationOutputResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetPreparationOutputRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPreparationOutputRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetPreparationOutputRequest._() : super();
  factory GetPreparationOutputRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory GetPreparationOutputRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPreparationOutputRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPreparationOutputRequest clone() => GetPreparationOutputRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPreparationOutputRequest copyWith(void Function(GetPreparationOutputRequest) updates) => super.copyWith((message) => updates(message as GetPreparationOutputRequest)) as GetPreparationOutputRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPreparationOutputRequest create() => GetPreparationOutputRequest._();
  GetPreparationOutputRequest createEmptyInstance() => create();
  static $pb.PbList<GetPreparationOutputRequest> createRepeated() => $pb.PbList<GetPreparationOutputRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPreparationOutputRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPreparationOutputRequest>(create);
  static GetPreparationOutputRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetPreparationOutputResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPreparationOutputResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..hasRequiredFields = false
  ;

  GetPreparationOutputResponse._() : super();
  factory GetPreparationOutputResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }
  factory GetPreparationOutputResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPreparationOutputResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPreparationOutputResponse clone() => GetPreparationOutputResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPreparationOutputResponse copyWith(void Function(GetPreparationOutputResponse) updates) => super.copyWith((message) => updates(message as GetPreparationOutputResponse)) as GetPreparationOutputResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPreparationOutputResponse create() => GetPreparationOutputResponse._();
  GetPreparationOutputResponse createEmptyInstance() => create();
  static $pb.PbList<GetPreparationOutputResponse> createRepeated() => $pb.PbList<GetPreparationOutputResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPreparationOutputResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPreparationOutputResponse>(create);
  static GetPreparationOutputResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
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
    ..hasRequiredFields = false
  ;

  GetCompileOutputResponse._() : super();
  factory GetCompileOutputResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
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
  factory GetRunErrorResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetRunErrorResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetRunErrorResponse clone() => GetRunErrorResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetRunErrorResponse copyWith(void Function(GetRunErrorResponse) updates) => super.copyWith((message) => updates(message as GetRunErrorResponse)) as GetRunErrorResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetRunErrorResponse create() => GetRunErrorResponse._();
  GetRunErrorResponse createEmptyInstance() => create();
  static $pb.PbList<GetRunErrorResponse> createRepeated() => $pb.PbList<GetRunErrorResponse>();
  @$core.pragma('dart2js:noInline')
  static GetRunErrorResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetRunErrorResponse>(create);
  static GetRunErrorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetLogsRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetLogsRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
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
  factory GetLogsRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetLogsRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetLogsRequest clone() => GetLogsRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetLogsRequest copyWith(void Function(GetLogsRequest) updates) => super.copyWith((message) => updates(message as GetLogsRequest)) as GetLogsRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetLogsRequest create() => GetLogsRequest._();
  GetLogsRequest createEmptyInstance() => create();
  static $pb.PbList<GetLogsRequest> createRepeated() => $pb.PbList<GetLogsRequest>();
  @$core.pragma('dart2js:noInline')
  static GetLogsRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetLogsRequest>(create);
  static GetLogsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetLogsResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetLogsResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
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
  factory GetLogsResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetLogsResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetLogsResponse clone() => GetLogsResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetLogsResponse copyWith(void Function(GetLogsResponse) updates) => super.copyWith((message) => updates(message as GetLogsResponse)) as GetLogsResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetLogsResponse create() => GetLogsResponse._();
  GetLogsResponse createEmptyInstance() => create();
  static $pb.PbList<GetLogsResponse> createRepeated() => $pb.PbList<GetLogsResponse>();
  @$core.pragma('dart2js:noInline')
  static GetLogsResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetLogsResponse>(create);
  static GetLogsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetGraphRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetGraphRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
    ..hasRequiredFields = false
  ;

  GetGraphRequest._() : super();
  factory GetGraphRequest({
    $core.String? pipelineUuid,
  }) {
    final _result = create();
    if (pipelineUuid != null) {
      _result.pipelineUuid = pipelineUuid;
    }
    return _result;
  }
  factory GetGraphRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetGraphRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetGraphRequest clone() => GetGraphRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetGraphRequest copyWith(void Function(GetGraphRequest) updates) => super.copyWith((message) => updates(message as GetGraphRequest)) as GetGraphRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetGraphRequest create() => GetGraphRequest._();
  GetGraphRequest createEmptyInstance() => create();
  static $pb.PbList<GetGraphRequest> createRepeated() => $pb.PbList<GetGraphRequest>();
  @$core.pragma('dart2js:noInline')
  static GetGraphRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetGraphRequest>(create);
  static GetGraphRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get pipelineUuid => $_getSZ(0);
  @$pb.TagNumber(1)
  set pipelineUuid($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasPipelineUuid() => $_has(0);
  @$pb.TagNumber(1)
  void clearPipelineUuid() => clearField(1);
}

class GetGraphResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetGraphResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'graph')
    ..hasRequiredFields = false
  ;

  GetGraphResponse._() : super();
  factory GetGraphResponse({
    $core.String? graph,
  }) {
    final _result = create();
    if (graph != null) {
      _result.graph = graph;
    }
    return _result;
  }
  factory GetGraphResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetGraphResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetGraphResponse clone() => GetGraphResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetGraphResponse copyWith(void Function(GetGraphResponse) updates) => super.copyWith((message) => updates(message as GetGraphResponse)) as GetGraphResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetGraphResponse create() => GetGraphResponse._();
  GetGraphResponse createEmptyInstance() => create();
  static $pb.PbList<GetGraphResponse> createRepeated() => $pb.PbList<GetGraphResponse>();
  @$core.pragma('dart2js:noInline')
  static GetGraphResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetGraphResponse>(create);
  static GetGraphResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get graph => $_getSZ(0);
  @$pb.TagNumber(1)
  set graph($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasGraph() => $_has(0);
  @$pb.TagNumber(1)
  void clearGraph() => clearField(1);
}

class CancelRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'CancelRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineUuid')
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

class PrecompiledObject extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'PrecompiledObject', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..aOS(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'name')
    ..aOS(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'description')
    ..e<PrecompiledObjectType>(4, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'type', $pb.PbFieldType.OE, defaultOrMaker: PrecompiledObjectType.PRECOMPILED_OBJECT_TYPE_UNSPECIFIED, valueOf: PrecompiledObjectType.valueOf, enumValues: PrecompiledObjectType.values)
    ..aOS(5, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineOptions')
    ..aOS(6, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'link')
    ..aOB(7, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'multifile')
    ..a<$core.int>(8, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'contextLine', $pb.PbFieldType.O3)
    ..aOB(9, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'defaultExample')
    ..e<Sdk>(10, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..e<Complexity>(11, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'complexity', $pb.PbFieldType.OE, defaultOrMaker: Complexity.COMPLEXITY_UNSPECIFIED, valueOf: Complexity.valueOf, enumValues: Complexity.values)
    ..pPS(12, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'tags')
    ..pc<Dataset>(13, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'datasets', $pb.PbFieldType.PM, subBuilder: Dataset.create)
    ..aOS(14, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'urlVcs')
    ..aOS(15, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'urlNotebook')
    ..aOB(16, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'alwaysRun')
    ..aOB(17, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'neverRun')
    ..hasRequiredFields = false
  ;

  PrecompiledObject._() : super();
  factory PrecompiledObject({
    $core.String? cloudPath,
    $core.String? name,
    $core.String? description,
    PrecompiledObjectType? type,
    $core.String? pipelineOptions,
    $core.String? link,
    $core.bool? multifile,
    $core.int? contextLine,
    $core.bool? defaultExample,
    Sdk? sdk,
    Complexity? complexity,
    $core.Iterable<$core.String>? tags,
    $core.Iterable<Dataset>? datasets,
    $core.String? urlVcs,
    $core.String? urlNotebook,
    $core.bool? alwaysRun,
    $core.bool? neverRun,
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
    if (link != null) {
      _result.link = link;
    }
    if (multifile != null) {
      _result.multifile = multifile;
    }
    if (contextLine != null) {
      _result.contextLine = contextLine;
    }
    if (defaultExample != null) {
      _result.defaultExample = defaultExample;
    }
    if (sdk != null) {
      _result.sdk = sdk;
    }
    if (complexity != null) {
      _result.complexity = complexity;
    }
    if (tags != null) {
      _result.tags.addAll(tags);
    }
    if (datasets != null) {
      _result.datasets.addAll(datasets);
    }
    if (urlVcs != null) {
      _result.urlVcs = urlVcs;
    }
    if (urlNotebook != null) {
      _result.urlNotebook = urlNotebook;
    }
    if (alwaysRun != null) {
      _result.alwaysRun = alwaysRun;
    }
    if (neverRun != null) {
      _result.neverRun = neverRun;
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
  set type(PrecompiledObjectType v) { setField(4, v); }
  @$pb.TagNumber(4)
  $core.bool hasType() => $_has(3);
  @$pb.TagNumber(4)
  void clearType() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get pipelineOptions => $_getSZ(4);
  @$pb.TagNumber(5)
  set pipelineOptions($core.String v) { $_setString(4, v); }
  @$pb.TagNumber(5)
  $core.bool hasPipelineOptions() => $_has(4);
  @$pb.TagNumber(5)
  void clearPipelineOptions() => clearField(5);

  @$pb.TagNumber(6)
  $core.String get link => $_getSZ(5);
  @$pb.TagNumber(6)
  set link($core.String v) { $_setString(5, v); }
  @$pb.TagNumber(6)
  $core.bool hasLink() => $_has(5);
  @$pb.TagNumber(6)
  void clearLink() => clearField(6);

  @$pb.TagNumber(7)
  $core.bool get multifile => $_getBF(6);
  @$pb.TagNumber(7)
  set multifile($core.bool v) { $_setBool(6, v); }
  @$pb.TagNumber(7)
  $core.bool hasMultifile() => $_has(6);
  @$pb.TagNumber(7)
  void clearMultifile() => clearField(7);

  @$pb.TagNumber(8)
  $core.int get contextLine => $_getIZ(7);
  @$pb.TagNumber(8)
  set contextLine($core.int v) { $_setSignedInt32(7, v); }
  @$pb.TagNumber(8)
  $core.bool hasContextLine() => $_has(7);
  @$pb.TagNumber(8)
  void clearContextLine() => clearField(8);

  @$pb.TagNumber(9)
  $core.bool get defaultExample => $_getBF(8);
  @$pb.TagNumber(9)
  set defaultExample($core.bool v) { $_setBool(8, v); }
  @$pb.TagNumber(9)
  $core.bool hasDefaultExample() => $_has(8);
  @$pb.TagNumber(9)
  void clearDefaultExample() => clearField(9);

  @$pb.TagNumber(10)
  Sdk get sdk => $_getN(9);
  @$pb.TagNumber(10)
  set sdk(Sdk v) { setField(10, v); }
  @$pb.TagNumber(10)
  $core.bool hasSdk() => $_has(9);
  @$pb.TagNumber(10)
  void clearSdk() => clearField(10);

  @$pb.TagNumber(11)
  Complexity get complexity => $_getN(10);
  @$pb.TagNumber(11)
  set complexity(Complexity v) { setField(11, v); }
  @$pb.TagNumber(11)
  $core.bool hasComplexity() => $_has(10);
  @$pb.TagNumber(11)
  void clearComplexity() => clearField(11);

  @$pb.TagNumber(12)
  $core.List<$core.String> get tags => $_getList(11);

  @$pb.TagNumber(13)
  $core.List<Dataset> get datasets => $_getList(12);

  @$pb.TagNumber(14)
  $core.String get urlVcs => $_getSZ(13);
  @$pb.TagNumber(14)
  set urlVcs($core.String v) { $_setString(13, v); }
  @$pb.TagNumber(14)
  $core.bool hasUrlVcs() => $_has(13);
  @$pb.TagNumber(14)
  void clearUrlVcs() => clearField(14);

  @$pb.TagNumber(15)
  $core.String get urlNotebook => $_getSZ(14);
  @$pb.TagNumber(15)
  set urlNotebook($core.String v) { $_setString(14, v); }
  @$pb.TagNumber(15)
  $core.bool hasUrlNotebook() => $_has(14);
  @$pb.TagNumber(15)
  void clearUrlNotebook() => clearField(15);

  @$pb.TagNumber(16)
  $core.bool get alwaysRun => $_getBF(15);
  @$pb.TagNumber(16)
  set alwaysRun($core.bool v) { $_setBool(15, v); }
  @$pb.TagNumber(16)
  $core.bool hasAlwaysRun() => $_has(15);
  @$pb.TagNumber(16)
  void clearAlwaysRun() => clearField(16);

  @$pb.TagNumber(17)
  $core.bool get neverRun => $_getBF(16);
  @$pb.TagNumber(17)
  set neverRun($core.bool v) { $_setBool(16, v); }
  @$pb.TagNumber(17)
  $core.bool hasNeverRun() => $_has(16);
  @$pb.TagNumber(17)
  void clearNeverRun() => clearField(17);
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

class GetPrecompiledObjectCodeRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectCodeRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectCodeRequest._() : super();
  factory GetPrecompiledObjectCodeRequest({
    $core.String? cloudPath,
  }) {
    final _result = create();
    if (cloudPath != null) {
      _result.cloudPath = cloudPath;
    }
    return _result;
  }
  factory GetPrecompiledObjectCodeRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectCodeRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectCodeRequest clone() => GetPrecompiledObjectCodeRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectCodeRequest copyWith(void Function(GetPrecompiledObjectCodeRequest) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectCodeRequest)) as GetPrecompiledObjectCodeRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectCodeRequest create() => GetPrecompiledObjectCodeRequest._();
  GetPrecompiledObjectCodeRequest createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectCodeRequest> createRepeated() => $pb.PbList<GetPrecompiledObjectCodeRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectCodeRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectCodeRequest>(create);
  static GetPrecompiledObjectCodeRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cloudPath => $_getSZ(0);
  @$pb.TagNumber(1)
  set cloudPath($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCloudPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearCloudPath() => clearField(1);
}

class GetPrecompiledObjectOutputRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectOutputRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectOutputRequest._() : super();
  factory GetPrecompiledObjectOutputRequest({
    $core.String? cloudPath,
  }) {
    final _result = create();
    if (cloudPath != null) {
      _result.cloudPath = cloudPath;
    }
    return _result;
  }
  factory GetPrecompiledObjectOutputRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectOutputRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectOutputRequest clone() => GetPrecompiledObjectOutputRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectOutputRequest copyWith(void Function(GetPrecompiledObjectOutputRequest) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectOutputRequest)) as GetPrecompiledObjectOutputRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectOutputRequest create() => GetPrecompiledObjectOutputRequest._();
  GetPrecompiledObjectOutputRequest createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectOutputRequest> createRepeated() => $pb.PbList<GetPrecompiledObjectOutputRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectOutputRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectOutputRequest>(create);
  static GetPrecompiledObjectOutputRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cloudPath => $_getSZ(0);
  @$pb.TagNumber(1)
  set cloudPath($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCloudPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearCloudPath() => clearField(1);
}

class GetPrecompiledObjectLogsRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectLogsRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectLogsRequest._() : super();
  factory GetPrecompiledObjectLogsRequest({
    $core.String? cloudPath,
  }) {
    final _result = create();
    if (cloudPath != null) {
      _result.cloudPath = cloudPath;
    }
    return _result;
  }
  factory GetPrecompiledObjectLogsRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectLogsRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectLogsRequest clone() => GetPrecompiledObjectLogsRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectLogsRequest copyWith(void Function(GetPrecompiledObjectLogsRequest) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectLogsRequest)) as GetPrecompiledObjectLogsRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectLogsRequest create() => GetPrecompiledObjectLogsRequest._();
  GetPrecompiledObjectLogsRequest createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectLogsRequest> createRepeated() => $pb.PbList<GetPrecompiledObjectLogsRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectLogsRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectLogsRequest>(create);
  static GetPrecompiledObjectLogsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cloudPath => $_getSZ(0);
  @$pb.TagNumber(1)
  set cloudPath($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCloudPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearCloudPath() => clearField(1);
}

class GetPrecompiledObjectGraphRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectGraphRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'cloudPath')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectGraphRequest._() : super();
  factory GetPrecompiledObjectGraphRequest({
    $core.String? cloudPath,
  }) {
    final _result = create();
    if (cloudPath != null) {
      _result.cloudPath = cloudPath;
    }
    return _result;
  }
  factory GetPrecompiledObjectGraphRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectGraphRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectGraphRequest clone() => GetPrecompiledObjectGraphRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectGraphRequest copyWith(void Function(GetPrecompiledObjectGraphRequest) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectGraphRequest)) as GetPrecompiledObjectGraphRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectGraphRequest create() => GetPrecompiledObjectGraphRequest._();
  GetPrecompiledObjectGraphRequest createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectGraphRequest> createRepeated() => $pb.PbList<GetPrecompiledObjectGraphRequest>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectGraphRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectGraphRequest>(create);
  static GetPrecompiledObjectGraphRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get cloudPath => $_getSZ(0);
  @$pb.TagNumber(1)
  set cloudPath($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasCloudPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearCloudPath() => clearField(1);
}

class GetDefaultPrecompiledObjectRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetDefaultPrecompiledObjectRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..e<Sdk>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..hasRequiredFields = false
  ;

  GetDefaultPrecompiledObjectRequest._() : super();
  factory GetDefaultPrecompiledObjectRequest({
    Sdk? sdk,
  }) {
    final _result = create();
    if (sdk != null) {
      _result.sdk = sdk;
    }
    return _result;
  }
  factory GetDefaultPrecompiledObjectRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetDefaultPrecompiledObjectRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetDefaultPrecompiledObjectRequest clone() => GetDefaultPrecompiledObjectRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetDefaultPrecompiledObjectRequest copyWith(void Function(GetDefaultPrecompiledObjectRequest) updates) => super.copyWith((message) => updates(message as GetDefaultPrecompiledObjectRequest)) as GetDefaultPrecompiledObjectRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetDefaultPrecompiledObjectRequest create() => GetDefaultPrecompiledObjectRequest._();
  GetDefaultPrecompiledObjectRequest createEmptyInstance() => create();
  static $pb.PbList<GetDefaultPrecompiledObjectRequest> createRepeated() => $pb.PbList<GetDefaultPrecompiledObjectRequest>();
  @$core.pragma('dart2js:noInline')
  static GetDefaultPrecompiledObjectRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetDefaultPrecompiledObjectRequest>(create);
  static GetDefaultPrecompiledObjectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  Sdk get sdk => $_getN(0);
  @$pb.TagNumber(1)
  set sdk(Sdk v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasSdk() => $_has(0);
  @$pb.TagNumber(1)
  void clearSdk() => clearField(1);
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

class GetPrecompiledObjectResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOM<PrecompiledObject>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'precompiledObject', subBuilder: PrecompiledObject.create)
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectResponse._() : super();
  factory GetPrecompiledObjectResponse({
    PrecompiledObject? precompiledObject,
  }) {
    final _result = create();
    if (precompiledObject != null) {
      _result.precompiledObject = precompiledObject;
    }
    return _result;
  }
  factory GetPrecompiledObjectResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectResponse clone() => GetPrecompiledObjectResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectResponse copyWith(void Function(GetPrecompiledObjectResponse) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectResponse)) as GetPrecompiledObjectResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectResponse create() => GetPrecompiledObjectResponse._();
  GetPrecompiledObjectResponse createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectResponse> createRepeated() => $pb.PbList<GetPrecompiledObjectResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectResponse>(create);
  static GetPrecompiledObjectResponse? _defaultInstance;

  @$pb.TagNumber(1)
  PrecompiledObject get precompiledObject => $_getN(0);
  @$pb.TagNumber(1)
  set precompiledObject(PrecompiledObject v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasPrecompiledObject() => $_has(0);
  @$pb.TagNumber(1)
  void clearPrecompiledObject() => clearField(1);
  @$pb.TagNumber(1)
  PrecompiledObject ensurePrecompiledObject() => $_ensure(0);
}

class GetPrecompiledObjectCodeResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectCodeResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'code')
    ..pc<SnippetFile>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'files', $pb.PbFieldType.PM, subBuilder: SnippetFile.create)
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectCodeResponse._() : super();
  factory GetPrecompiledObjectCodeResponse({
    $core.String? code,
    $core.Iterable<SnippetFile>? files,
  }) {
    final _result = create();
    if (code != null) {
      _result.code = code;
    }
    if (files != null) {
      _result.files.addAll(files);
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

  @$pb.TagNumber(2)
  $core.List<SnippetFile> get files => $_getList(1);
}

class GetPrecompiledObjectOutputResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectOutputResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectOutputResponse._() : super();
  factory GetPrecompiledObjectOutputResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }
  factory GetPrecompiledObjectOutputResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectOutputResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectOutputResponse clone() => GetPrecompiledObjectOutputResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectOutputResponse copyWith(void Function(GetPrecompiledObjectOutputResponse) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectOutputResponse)) as GetPrecompiledObjectOutputResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectOutputResponse create() => GetPrecompiledObjectOutputResponse._();
  GetPrecompiledObjectOutputResponse createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectOutputResponse> createRepeated() => $pb.PbList<GetPrecompiledObjectOutputResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectOutputResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectOutputResponse>(create);
  static GetPrecompiledObjectOutputResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetPrecompiledObjectLogsResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectLogsResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'output')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectLogsResponse._() : super();
  factory GetPrecompiledObjectLogsResponse({
    $core.String? output,
  }) {
    final _result = create();
    if (output != null) {
      _result.output = output;
    }
    return _result;
  }
  factory GetPrecompiledObjectLogsResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectLogsResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectLogsResponse clone() => GetPrecompiledObjectLogsResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectLogsResponse copyWith(void Function(GetPrecompiledObjectLogsResponse) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectLogsResponse)) as GetPrecompiledObjectLogsResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectLogsResponse create() => GetPrecompiledObjectLogsResponse._();
  GetPrecompiledObjectLogsResponse createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectLogsResponse> createRepeated() => $pb.PbList<GetPrecompiledObjectLogsResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectLogsResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectLogsResponse>(create);
  static GetPrecompiledObjectLogsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get output => $_getSZ(0);
  @$pb.TagNumber(1)
  set output($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => clearField(1);
}

class GetPrecompiledObjectGraphResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetPrecompiledObjectGraphResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'graph')
    ..hasRequiredFields = false
  ;

  GetPrecompiledObjectGraphResponse._() : super();
  factory GetPrecompiledObjectGraphResponse({
    $core.String? graph,
  }) {
    final _result = create();
    if (graph != null) {
      _result.graph = graph;
    }
    return _result;
  }
  factory GetPrecompiledObjectGraphResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetPrecompiledObjectGraphResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectGraphResponse clone() => GetPrecompiledObjectGraphResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetPrecompiledObjectGraphResponse copyWith(void Function(GetPrecompiledObjectGraphResponse) updates) => super.copyWith((message) => updates(message as GetPrecompiledObjectGraphResponse)) as GetPrecompiledObjectGraphResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectGraphResponse create() => GetPrecompiledObjectGraphResponse._();
  GetPrecompiledObjectGraphResponse createEmptyInstance() => create();
  static $pb.PbList<GetPrecompiledObjectGraphResponse> createRepeated() => $pb.PbList<GetPrecompiledObjectGraphResponse>();
  @$core.pragma('dart2js:noInline')
  static GetPrecompiledObjectGraphResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetPrecompiledObjectGraphResponse>(create);
  static GetPrecompiledObjectGraphResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get graph => $_getSZ(0);
  @$pb.TagNumber(1)
  set graph($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasGraph() => $_has(0);
  @$pb.TagNumber(1)
  void clearGraph() => clearField(1);
}

class GetDefaultPrecompiledObjectResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetDefaultPrecompiledObjectResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOM<PrecompiledObject>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'precompiledObject', subBuilder: PrecompiledObject.create)
    ..hasRequiredFields = false
  ;

  GetDefaultPrecompiledObjectResponse._() : super();
  factory GetDefaultPrecompiledObjectResponse({
    PrecompiledObject? precompiledObject,
  }) {
    final _result = create();
    if (precompiledObject != null) {
      _result.precompiledObject = precompiledObject;
    }
    return _result;
  }
  factory GetDefaultPrecompiledObjectResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetDefaultPrecompiledObjectResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetDefaultPrecompiledObjectResponse clone() => GetDefaultPrecompiledObjectResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetDefaultPrecompiledObjectResponse copyWith(void Function(GetDefaultPrecompiledObjectResponse) updates) => super.copyWith((message) => updates(message as GetDefaultPrecompiledObjectResponse)) as GetDefaultPrecompiledObjectResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetDefaultPrecompiledObjectResponse create() => GetDefaultPrecompiledObjectResponse._();
  GetDefaultPrecompiledObjectResponse createEmptyInstance() => create();
  static $pb.PbList<GetDefaultPrecompiledObjectResponse> createRepeated() => $pb.PbList<GetDefaultPrecompiledObjectResponse>();
  @$core.pragma('dart2js:noInline')
  static GetDefaultPrecompiledObjectResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetDefaultPrecompiledObjectResponse>(create);
  static GetDefaultPrecompiledObjectResponse? _defaultInstance;

  @$pb.TagNumber(1)
  PrecompiledObject get precompiledObject => $_getN(0);
  @$pb.TagNumber(1)
  set precompiledObject(PrecompiledObject v) { setField(1, v); }
  @$pb.TagNumber(1)
  $core.bool hasPrecompiledObject() => $_has(0);
  @$pb.TagNumber(1)
  void clearPrecompiledObject() => clearField(1);
  @$pb.TagNumber(1)
  PrecompiledObject ensurePrecompiledObject() => $_ensure(0);
}

class SnippetFile extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'SnippetFile', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'name')
    ..aOS(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'content')
    ..aOB(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'isMain')
    ..hasRequiredFields = false
  ;

  SnippetFile._() : super();
  factory SnippetFile({
    $core.String? name,
    $core.String? content,
    $core.bool? isMain,
  }) {
    final _result = create();
    if (name != null) {
      _result.name = name;
    }
    if (content != null) {
      _result.content = content;
    }
    if (isMain != null) {
      _result.isMain = isMain;
    }
    return _result;
  }
  factory SnippetFile.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory SnippetFile.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  SnippetFile clone() => SnippetFile()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  SnippetFile copyWith(void Function(SnippetFile) updates) => super.copyWith((message) => updates(message as SnippetFile)) as SnippetFile; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static SnippetFile create() => SnippetFile._();
  SnippetFile createEmptyInstance() => create();
  static $pb.PbList<SnippetFile> createRepeated() => $pb.PbList<SnippetFile>();
  @$core.pragma('dart2js:noInline')
  static SnippetFile getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SnippetFile>(create);
  static SnippetFile? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get content => $_getSZ(1);
  @$pb.TagNumber(2)
  set content($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasContent() => $_has(1);
  @$pb.TagNumber(2)
  void clearContent() => clearField(2);

  @$pb.TagNumber(3)
  $core.bool get isMain => $_getBF(2);
  @$pb.TagNumber(3)
  set isMain($core.bool v) { $_setBool(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasIsMain() => $_has(2);
  @$pb.TagNumber(3)
  void clearIsMain() => clearField(3);
}

class SaveSnippetRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'SaveSnippetRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..pc<SnippetFile>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'files', $pb.PbFieldType.PM, subBuilder: SnippetFile.create)
    ..e<Sdk>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..aOS(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineOptions')
    ..e<Complexity>(4, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'complexity', $pb.PbFieldType.OE, defaultOrMaker: Complexity.COMPLEXITY_UNSPECIFIED, valueOf: Complexity.valueOf, enumValues: Complexity.values)
    ..aOS(5, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'persistenceKey')
    ..hasRequiredFields = false
  ;

  SaveSnippetRequest._() : super();
  factory SaveSnippetRequest({
    $core.Iterable<SnippetFile>? files,
    Sdk? sdk,
    $core.String? pipelineOptions,
    Complexity? complexity,
    $core.String? persistenceKey,
  }) {
    final _result = create();
    if (files != null) {
      _result.files.addAll(files);
    }
    if (sdk != null) {
      _result.sdk = sdk;
    }
    if (pipelineOptions != null) {
      _result.pipelineOptions = pipelineOptions;
    }
    if (complexity != null) {
      _result.complexity = complexity;
    }
    if (persistenceKey != null) {
      _result.persistenceKey = persistenceKey;
    }
    return _result;
  }
  factory SaveSnippetRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory SaveSnippetRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  SaveSnippetRequest clone() => SaveSnippetRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  SaveSnippetRequest copyWith(void Function(SaveSnippetRequest) updates) => super.copyWith((message) => updates(message as SaveSnippetRequest)) as SaveSnippetRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static SaveSnippetRequest create() => SaveSnippetRequest._();
  SaveSnippetRequest createEmptyInstance() => create();
  static $pb.PbList<SaveSnippetRequest> createRepeated() => $pb.PbList<SaveSnippetRequest>();
  @$core.pragma('dart2js:noInline')
  static SaveSnippetRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SaveSnippetRequest>(create);
  static SaveSnippetRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<SnippetFile> get files => $_getList(0);

  @$pb.TagNumber(2)
  Sdk get sdk => $_getN(1);
  @$pb.TagNumber(2)
  set sdk(Sdk v) { setField(2, v); }
  @$pb.TagNumber(2)
  $core.bool hasSdk() => $_has(1);
  @$pb.TagNumber(2)
  void clearSdk() => clearField(2);

  @$pb.TagNumber(3)
  $core.String get pipelineOptions => $_getSZ(2);
  @$pb.TagNumber(3)
  set pipelineOptions($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasPipelineOptions() => $_has(2);
  @$pb.TagNumber(3)
  void clearPipelineOptions() => clearField(3);

  @$pb.TagNumber(4)
  Complexity get complexity => $_getN(3);
  @$pb.TagNumber(4)
  set complexity(Complexity v) { setField(4, v); }
  @$pb.TagNumber(4)
  $core.bool hasComplexity() => $_has(3);
  @$pb.TagNumber(4)
  void clearComplexity() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get persistenceKey => $_getSZ(4);
  @$pb.TagNumber(5)
  set persistenceKey($core.String v) { $_setString(4, v); }
  @$pb.TagNumber(5)
  $core.bool hasPersistenceKey() => $_has(4);
  @$pb.TagNumber(5)
  void clearPersistenceKey() => clearField(5);
}

class SaveSnippetResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'SaveSnippetResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'id')
    ..hasRequiredFields = false
  ;

  SaveSnippetResponse._() : super();
  factory SaveSnippetResponse({
    $core.String? id,
  }) {
    final _result = create();
    if (id != null) {
      _result.id = id;
    }
    return _result;
  }
  factory SaveSnippetResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory SaveSnippetResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  SaveSnippetResponse clone() => SaveSnippetResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  SaveSnippetResponse copyWith(void Function(SaveSnippetResponse) updates) => super.copyWith((message) => updates(message as SaveSnippetResponse)) as SaveSnippetResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static SaveSnippetResponse create() => SaveSnippetResponse._();
  SaveSnippetResponse createEmptyInstance() => create();
  static $pb.PbList<SaveSnippetResponse> createRepeated() => $pb.PbList<SaveSnippetResponse>();
  @$core.pragma('dart2js:noInline')
  static SaveSnippetResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SaveSnippetResponse>(create);
  static SaveSnippetResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => clearField(1);
}

class GetSnippetRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetSnippetRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'id')
    ..hasRequiredFields = false
  ;

  GetSnippetRequest._() : super();
  factory GetSnippetRequest({
    $core.String? id,
  }) {
    final _result = create();
    if (id != null) {
      _result.id = id;
    }
    return _result;
  }
  factory GetSnippetRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetSnippetRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetSnippetRequest clone() => GetSnippetRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetSnippetRequest copyWith(void Function(GetSnippetRequest) updates) => super.copyWith((message) => updates(message as GetSnippetRequest)) as GetSnippetRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetSnippetRequest create() => GetSnippetRequest._();
  GetSnippetRequest createEmptyInstance() => create();
  static $pb.PbList<GetSnippetRequest> createRepeated() => $pb.PbList<GetSnippetRequest>();
  @$core.pragma('dart2js:noInline')
  static GetSnippetRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetSnippetRequest>(create);
  static GetSnippetRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => clearField(1);
}

class GetSnippetResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetSnippetResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..pc<SnippetFile>(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'files', $pb.PbFieldType.PM, subBuilder: SnippetFile.create)
    ..e<Sdk>(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'sdk', $pb.PbFieldType.OE, defaultOrMaker: Sdk.SDK_UNSPECIFIED, valueOf: Sdk.valueOf, enumValues: Sdk.values)
    ..aOS(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'pipelineOptions')
    ..e<Complexity>(4, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'complexity', $pb.PbFieldType.OE, defaultOrMaker: Complexity.COMPLEXITY_UNSPECIFIED, valueOf: Complexity.valueOf, enumValues: Complexity.values)
    ..hasRequiredFields = false
  ;

  GetSnippetResponse._() : super();
  factory GetSnippetResponse({
    $core.Iterable<SnippetFile>? files,
    Sdk? sdk,
    $core.String? pipelineOptions,
    Complexity? complexity,
  }) {
    final _result = create();
    if (files != null) {
      _result.files.addAll(files);
    }
    if (sdk != null) {
      _result.sdk = sdk;
    }
    if (pipelineOptions != null) {
      _result.pipelineOptions = pipelineOptions;
    }
    if (complexity != null) {
      _result.complexity = complexity;
    }
    return _result;
  }
  factory GetSnippetResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetSnippetResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetSnippetResponse clone() => GetSnippetResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetSnippetResponse copyWith(void Function(GetSnippetResponse) updates) => super.copyWith((message) => updates(message as GetSnippetResponse)) as GetSnippetResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetSnippetResponse create() => GetSnippetResponse._();
  GetSnippetResponse createEmptyInstance() => create();
  static $pb.PbList<GetSnippetResponse> createRepeated() => $pb.PbList<GetSnippetResponse>();
  @$core.pragma('dart2js:noInline')
  static GetSnippetResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetSnippetResponse>(create);
  static GetSnippetResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<SnippetFile> get files => $_getList(0);

  @$pb.TagNumber(2)
  Sdk get sdk => $_getN(1);
  @$pb.TagNumber(2)
  set sdk(Sdk v) { setField(2, v); }
  @$pb.TagNumber(2)
  $core.bool hasSdk() => $_has(1);
  @$pb.TagNumber(2)
  void clearSdk() => clearField(2);

  @$pb.TagNumber(3)
  $core.String get pipelineOptions => $_getSZ(2);
  @$pb.TagNumber(3)
  set pipelineOptions($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasPipelineOptions() => $_has(2);
  @$pb.TagNumber(3)
  void clearPipelineOptions() => clearField(3);

  @$pb.TagNumber(4)
  Complexity get complexity => $_getN(3);
  @$pb.TagNumber(4)
  set complexity(Complexity v) { setField(4, v); }
  @$pb.TagNumber(4)
  $core.bool hasComplexity() => $_has(3);
  @$pb.TagNumber(4)
  void clearComplexity() => clearField(4);
}

class GetMetadataRequest extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetMetadataRequest', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..hasRequiredFields = false
  ;

  GetMetadataRequest._() : super();
  factory GetMetadataRequest() => create();
  factory GetMetadataRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetMetadataRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetMetadataRequest clone() => GetMetadataRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetMetadataRequest copyWith(void Function(GetMetadataRequest) updates) => super.copyWith((message) => updates(message as GetMetadataRequest)) as GetMetadataRequest; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetMetadataRequest create() => GetMetadataRequest._();
  GetMetadataRequest createEmptyInstance() => create();
  static $pb.PbList<GetMetadataRequest> createRepeated() => $pb.PbList<GetMetadataRequest>();
  @$core.pragma('dart2js:noInline')
  static GetMetadataRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetMetadataRequest>(create);
  static GetMetadataRequest? _defaultInstance;
}

class GetMetadataResponse extends $pb.GeneratedMessage {
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'GetMetadataResponse', package: const $pb.PackageName(const $core.bool.fromEnvironment('protobuf.omit_message_names') ? '' : 'api.v1'), createEmptyInstance: create)
    ..aOS(1, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'runnerSdk')
    ..aOS(2, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'buildCommitHash')
    ..aInt64(3, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'buildCommitTimestampSecondsSinceEpoch')
    ..aOS(4, const $core.bool.fromEnvironment('protobuf.omit_field_names') ? '' : 'beamSdkVersion')
    ..hasRequiredFields = false
  ;

  GetMetadataResponse._() : super();
  factory GetMetadataResponse({
    $core.String? runnerSdk,
    $core.String? buildCommitHash,
    $fixnum.Int64? buildCommitTimestampSecondsSinceEpoch,
    $core.String? beamSdkVersion,
  }) {
    final _result = create();
    if (runnerSdk != null) {
      _result.runnerSdk = runnerSdk;
    }
    if (buildCommitHash != null) {
      _result.buildCommitHash = buildCommitHash;
    }
    if (buildCommitTimestampSecondsSinceEpoch != null) {
      _result.buildCommitTimestampSecondsSinceEpoch = buildCommitTimestampSecondsSinceEpoch;
    }
    if (beamSdkVersion != null) {
      _result.beamSdkVersion = beamSdkVersion;
    }
    return _result;
  }
  factory GetMetadataResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory GetMetadataResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  GetMetadataResponse clone() => GetMetadataResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  GetMetadataResponse copyWith(void Function(GetMetadataResponse) updates) => super.copyWith((message) => updates(message as GetMetadataResponse)) as GetMetadataResponse; // ignore: deprecated_member_use
  $pb.BuilderInfo get info_ => _i;
  @$core.pragma('dart2js:noInline')
  static GetMetadataResponse create() => GetMetadataResponse._();
  GetMetadataResponse createEmptyInstance() => create();
  static $pb.PbList<GetMetadataResponse> createRepeated() => $pb.PbList<GetMetadataResponse>();
  @$core.pragma('dart2js:noInline')
  static GetMetadataResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetMetadataResponse>(create);
  static GetMetadataResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get runnerSdk => $_getSZ(0);
  @$pb.TagNumber(1)
  set runnerSdk($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasRunnerSdk() => $_has(0);
  @$pb.TagNumber(1)
  void clearRunnerSdk() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get buildCommitHash => $_getSZ(1);
  @$pb.TagNumber(2)
  set buildCommitHash($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasBuildCommitHash() => $_has(1);
  @$pb.TagNumber(2)
  void clearBuildCommitHash() => clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get buildCommitTimestampSecondsSinceEpoch => $_getI64(2);
  @$pb.TagNumber(3)
  set buildCommitTimestampSecondsSinceEpoch($fixnum.Int64 v) { $_setInt64(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasBuildCommitTimestampSecondsSinceEpoch() => $_has(2);
  @$pb.TagNumber(3)
  void clearBuildCommitTimestampSecondsSinceEpoch() => clearField(3);

  @$pb.TagNumber(4)
  $core.String get beamSdkVersion => $_getSZ(3);
  @$pb.TagNumber(4)
  set beamSdkVersion($core.String v) { $_setString(3, v); }
  @$pb.TagNumber(4)
  $core.bool hasBeamSdkVersion() => $_has(3);
  @$pb.TagNumber(4)
  void clearBeamSdkVersion() => clearField(4);
}

