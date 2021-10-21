///
//  Generated code. Do not modify.
//  source: playground.proto
//
// @dart = 2.12
// ignore_for_file: annotate_overrides,camel_case_types,unnecessary_const,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name,return_of_invalid_type,unnecessary_this,prefer_final_fields

// ignore_for_file: UNDEFINED_SHOWN_NAME
import 'dart:core' as $core;
import 'package:protobuf/protobuf.dart' as $pb;

class Sdk extends $pb.ProtobufEnum {
  static const Sdk SDK_UNSPECIFIED = Sdk._(0, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'SDK_UNSPECIFIED');
  static const Sdk SDK_JAVA = Sdk._(1, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'SDK_JAVA');
  static const Sdk SDK_GO = Sdk._(2, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'SDK_GO');
  static const Sdk SDK_PYTHON = Sdk._(3, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'SDK_PYTHON');
  static const Sdk SDK_SCIO = Sdk._(4, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'SDK_SCIO');

  static const $core.List<Sdk> values = <Sdk> [
    SDK_UNSPECIFIED,
    SDK_JAVA,
    SDK_GO,
    SDK_PYTHON,
    SDK_SCIO,
  ];

  static final $core.Map<$core.int, Sdk> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Sdk? valueOf($core.int value) => _byValue[value];

  const Sdk._($core.int v, $core.String n) : super(v, n);
}

class Status extends $pb.ProtobufEnum {
  static const Status STATUS_UNSPECIFIED = Status._(0, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'STATUS_UNSPECIFIED');
  static const Status STATUS_EXECUTING = Status._(1, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'STATUS_EXECUTING');
  static const Status STATUS_FINISHED = Status._(2, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'STATUS_FINISHED');
  static const Status STATUS_ERROR = Status._(3, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'STATUS_ERROR');

  static const $core.List<Status> values = <Status> [
    STATUS_UNSPECIFIED,
    STATUS_EXECUTING,
    STATUS_FINISHED,
    STATUS_ERROR,
  ];

  static final $core.Map<$core.int, Status> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Status? valueOf($core.int value) => _byValue[value];

  const Status._($core.int v, $core.String n) : super(v, n);
}

