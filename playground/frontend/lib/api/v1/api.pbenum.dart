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
  static const Status STATUS_COMPILE_ERROR = Status._(4, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'STATUS_COMPILE_ERROR');
  static const Status STATUS_RUN_TIMEOUT = Status._(5, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'STATUS_RUN_TIMEOUT');

  static const $core.List<Status> values = <Status> [
    STATUS_UNSPECIFIED,
    STATUS_EXECUTING,
    STATUS_FINISHED,
    STATUS_ERROR,
    STATUS_COMPILE_ERROR,
    STATUS_RUN_TIMEOUT,
  ];

  static final $core.Map<$core.int, Status> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Status? valueOf($core.int value) => _byValue[value];

  const Status._($core.int v, $core.String n) : super(v, n);
}

class ExampleType extends $pb.ProtobufEnum {
  static const ExampleType EXAMPLE_TYPE_DEFAULT = ExampleType._(0, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'EXAMPLE_TYPE_DEFAULT');
  static const ExampleType EXAMPLE_TYPE_KATA = ExampleType._(1, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'EXAMPLE_TYPE_KATA');
  static const ExampleType EXAMPLE_TYPE_UNIT_TEST = ExampleType._(2, const $core.bool.fromEnvironment('protobuf.omit_enum_names') ? '' : 'EXAMPLE_TYPE_UNIT_TEST');

  static const $core.List<ExampleType> values = <ExampleType> [
    EXAMPLE_TYPE_DEFAULT,
    EXAMPLE_TYPE_KATA,
    EXAMPLE_TYPE_UNIT_TEST,
  ];

  static final $core.Map<$core.int, ExampleType> _byValue = $pb.ProtobufEnum.initByValue(values);
  static ExampleType? valueOf($core.int value) => _byValue[value];

  const ExampleType._($core.int v, $core.String n) : super(v, n);
}

