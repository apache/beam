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
// ignore_for_file: annotate_overrides,camel_case_types,unnecessary_const,non_constant_identifier_names,library_prefixes,unused_import,unused_shown_name,return_of_invalid_type,unnecessary_this,prefer_final_fields,deprecated_member_use_from_same_package

import 'dart:core' as $core;
import 'dart:convert' as $convert;
import 'dart:typed_data' as $typed_data;
@$core.Deprecated('Use sdkDescriptor instead')
const Sdk$json = const {
  '1': 'Sdk',
  '2': const [
    const {'1': 'SDK_UNSPECIFIED', '2': 0},
    const {'1': 'SDK_JAVA', '2': 1},
    const {'1': 'SDK_GO', '2': 2},
    const {'1': 'SDK_PYTHON', '2': 3},
    const {'1': 'SDK_SCIO', '2': 4},
  ],
};

/// Descriptor for `Sdk`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List sdkDescriptor = $convert.base64Decode('CgNTZGsSEwoPU0RLX1VOU1BFQ0lGSUVEEAASDAoIU0RLX0pBVkEQARIKCgZTREtfR08QAhIOCgpTREtfUFlUSE9OEAMSDAoIU0RLX1NDSU8QBA==');
@$core.Deprecated('Use statusDescriptor instead')
const Status$json = const {
  '1': 'Status',
  '2': const [
    const {'1': 'STATUS_UNSPECIFIED', '2': 0},
    const {'1': 'STATUS_EXECUTING', '2': 1},
    const {'1': 'STATUS_FINISHED', '2': 2},
    const {'1': 'STATUS_ERROR', '2': 3},
  ],
};

/// Descriptor for `Status`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List statusDescriptor = $convert.base64Decode(
    'CgZTdGF0dXMSFgoSU1RBVFVTX1VOU1BFQ0lGSUVEEAASFAoQU1RBVFVTX0VYRUNVVElORxABEhMKD1NUQVRVU19GSU5JU0hFRBACEhAKDFNUQVRVU19FUlJPUhADEhgKFFNUQVRVU19DT01QSUxFX0VSUk9SEAQSFgoSU1RBVFVTX1JVTl9USU1FT1VUEAU=');
@$core.Deprecated('Use runCodeRequestDescriptor instead')
const RunCodeRequest$json = const {
  '1': 'RunCodeRequest',
  '2': const [
    const {'1': 'code', '3': 1, '4': 1, '5': 9, '10': 'code'},
    const {
      '1': 'sdk',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.api.v1.Sdk',
      '10': 'sdk'
    },
  ],
};

/// Descriptor for `RunCodeRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runCodeRequestDescriptor = $convert.base64Decode('Cg5SdW5Db2RlUmVxdWVzdBISCgRjb2RlGAEgASgJUgRjb2RlEh0KA3NkaxgCIAEoDjILLmFwaS52MS5TZGtSA3Nkaw==');
@$core.Deprecated('Use runCodeResponseDescriptor instead')
const RunCodeResponse$json = const {
  '1': 'RunCodeResponse',
  '2': const [
    const {'1': 'pipeline_uuid', '3': 1, '4': 1, '5': 9, '10': 'pipelineUuid'},
  ],
};

/// Descriptor for `RunCodeResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runCodeResponseDescriptor = $convert.base64Decode('Cg9SdW5Db2RlUmVzcG9uc2USIwoNcGlwZWxpbmVfdXVpZBgBIAEoCVIMcGlwZWxpbmVVdWlk');
@$core.Deprecated('Use checkStatusRequestDescriptor instead')
const CheckStatusRequest$json = const {
  '1': 'CheckStatusRequest',
  '2': const [
    const {'1': 'pipeline_uuid', '3': 1, '4': 1, '5': 9, '10': 'pipelineUuid'},
  ],
};

/// Descriptor for `CheckStatusRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkStatusRequestDescriptor = $convert.base64Decode('ChJDaGVja1N0YXR1c1JlcXVlc3QSIwoNcGlwZWxpbmVfdXVpZBgBIAEoCVIMcGlwZWxpbmVVdWlk');
@$core.Deprecated('Use checkStatusResponseDescriptor instead')
const CheckStatusResponse$json = const {
  '1': 'CheckStatusResponse',
  '2': const [
    const {'1': 'status', '3': 1, '4': 1, '5': 14, '6': '.api.v1.Status', '10': 'status'},
  ],
};

/// Descriptor for `CheckStatusResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkStatusResponseDescriptor = $convert.base64Decode('ChNDaGVja1N0YXR1c1Jlc3BvbnNlEiYKBnN0YXR1cxgBIAEoDjIOLmFwaS52MS5TdGF0dXNSBnN0YXR1cw==');
@$core.Deprecated('Use getCompileOutputRequestDescriptor instead')
const GetCompileOutputRequest$json = const {
  '1': 'GetCompileOutputRequest',
  '2': const [
    const {'1': 'pipeline_uuid', '3': 1, '4': 1, '5': 9, '10': 'pipelineUuid'},
  ],
};

/// Descriptor for `GetCompileOutputRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getCompileOutputRequestDescriptor = $convert.base64Decode('ChdHZXRDb21waWxlT3V0cHV0UmVxdWVzdBIjCg1waXBlbGluZV91dWlkGAEgASgJUgxwaXBlbGluZVV1aWQ=');
@$core.Deprecated('Use getCompileOutputResponseDescriptor instead')
const GetCompileOutputResponse$json = const {
  '1': 'GetCompileOutputResponse',
  '2': const [
    const {'1': 'output', '3': 1, '4': 1, '5': 9, '10': 'output'},
    const {'1': 'compilation_status', '3': 2, '4': 1, '5': 14, '6': '.api.v1.Status', '10': 'compilationStatus'},
  ],
};

/// Descriptor for `GetCompileOutputResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getCompileOutputResponseDescriptor = $convert.base64Decode('ChhHZXRDb21waWxlT3V0cHV0UmVzcG9uc2USFgoGb3V0cHV0GAEgASgJUgZvdXRwdXQSPQoSY29tcGlsYXRpb25fc3RhdHVzGAIgASgOMg4uYXBpLnYxLlN0YXR1c1IRY29tcGlsYXRpb25TdGF0dXM=');
@$core.Deprecated('Use getRunOutputRequestDescriptor instead')
const GetRunOutputRequest$json = const {
  '1': 'GetRunOutputRequest',
  '2': const [
    const {'1': 'pipeline_uuid', '3': 1, '4': 1, '5': 9, '10': 'pipelineUuid'},
  ],
};

/// Descriptor for `GetRunOutputRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getRunOutputRequestDescriptor = $convert.base64Decode('ChNHZXRSdW5PdXRwdXRSZXF1ZXN0EiMKDXBpcGVsaW5lX3V1aWQYASABKAlSDHBpcGVsaW5lVXVpZA==');
@$core.Deprecated('Use getRunOutputResponseDescriptor instead')
const GetRunOutputResponse$json = const {
  '1': 'GetRunOutputResponse',
  '2': const [
    const {'1': 'output', '3': 1, '4': 1, '5': 9, '10': 'output'},
    const {
      '1': 'compilation_status',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.api.v1.Status',
      '10': 'compilationStatus'
    },
  ],
};

/// Descriptor for `GetRunOutputResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getRunOutputResponseDescriptor = $convert.base64Decode(
    'ChRHZXRSdW5PdXRwdXRSZXNwb25zZRIWCgZvdXRwdXQYASABKAlSBm91dHB1dBI9ChJjb21waWxhdGlvbl9zdGF0dXMYAiABKA4yDi5hcGkudjEuU3RhdHVzUhFjb21waWxhdGlvblN0YXR1cw==');
@$core.Deprecated('Use getListOfExamplesRequestDescriptor instead')
const GetListOfExamplesRequest$json = const {
  '1': 'GetListOfExamplesRequest',
  '2': const [
    const {
      '1': 'sdk',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.api.v1.Sdk',
      '10': 'sdk'
    },
    const {'1': 'category', '3': 2, '4': 1, '5': 9, '10': 'category'},
  ],
};

/// Descriptor for `GetListOfExamplesRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getListOfExamplesRequestDescriptor =
    $convert.base64Decode(
        'ChhHZXRMaXN0T2ZFeGFtcGxlc1JlcXVlc3QSHQoDc2RrGAEgASgOMgsuYXBpLnYxLlNka1IDc2RrEhoKCGNhdGVnb3J5GAIgASgJUghjYXRlZ29yeQ==');
@$core.Deprecated('Use examplesDescriptor instead')
const Examples$json = const {
  '1': 'Examples',
  '2': const [
    const {'1': 'example', '3': 1, '4': 3, '5': 9, '10': 'example'},
  ],
};

/// Descriptor for `Examples`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List examplesDescriptor =
    $convert.base64Decode('CghFeGFtcGxlcxIYCgdleGFtcGxlGAEgAygJUgdleGFtcGxl');
@$core.Deprecated('Use categoryListDescriptor instead')
const CategoryList$json = const {
  '1': 'CategoryList',
  '2': const [
    const {
      '1': 'category_examples',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.api.v1.CategoryList.CategoryExamplesEntry',
      '10': 'categoryExamples'
    },
  ],
  '3': const [CategoryList_CategoryExamplesEntry$json],
};

@$core.Deprecated('Use categoryListDescriptor instead')
const CategoryList_CategoryExamplesEntry$json = const {
  '1': 'CategoryExamplesEntry',
  '2': const [
    const {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    const {
      '1': 'value',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.api.v1.Examples',
      '10': 'value'
    },
  ],
  '7': const {'7': true},
};

/// Descriptor for `CategoryList`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List categoryListDescriptor = $convert.base64Decode(
    'CgxDYXRlZ29yeUxpc3QSVwoRY2F0ZWdvcnlfZXhhbXBsZXMYASADKAsyKi5hcGkudjEuQ2F0ZWdvcnlMaXN0LkNhdGVnb3J5RXhhbXBsZXNFbnRyeVIQY2F0ZWdvcnlFeGFtcGxlcxpVChVDYXRlZ29yeUV4YW1wbGVzRW50cnkSEAoDa2V5GAEgASgJUgNrZXkSJgoFdmFsdWUYAiABKAsyEC5hcGkudjEuRXhhbXBsZXNSBXZhbHVlOgI4AQ==');
@$core.Deprecated('Use getListOfExamplesResponseDescriptor instead')
const GetListOfExamplesResponse$json = const {
  '1': 'GetListOfExamplesResponse',
  '2': const [
    const {
      '1': 'sdk_categories',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.api.v1.GetListOfExamplesResponse.SdkCategoriesEntry',
      '10': 'sdkCategories'
    },
  ],
  '3': const [GetListOfExamplesResponse_SdkCategoriesEntry$json],
};

@$core.Deprecated('Use getListOfExamplesResponseDescriptor instead')
const GetListOfExamplesResponse_SdkCategoriesEntry$json = const {
  '1': 'SdkCategoriesEntry',
  '2': const [
    const {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    const {
      '1': 'value',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.api.v1.CategoryList',
      '10': 'value'
    },
  ],
  '7': const {'7': true},
};

/// Descriptor for `GetListOfExamplesResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getListOfExamplesResponseDescriptor =
    $convert.base64Decode(
        'ChlHZXRMaXN0T2ZFeGFtcGxlc1Jlc3BvbnNlElsKDnNka19jYXRlZ29yaWVzGAEgAygLMjQuYXBpLnYxLkdldExpc3RPZkV4YW1wbGVzUmVzcG9uc2UuU2RrQ2F0ZWdvcmllc0VudHJ5Ug1zZGtDYXRlZ29yaWVzGlYKElNka0NhdGVnb3JpZXNFbnRyeRIQCgNrZXkYASABKAlSA2tleRIqCgV2YWx1ZRgCIAEoCzIULmFwaS52MS5DYXRlZ29yeUxpc3RSBXZhbHVlOgI4AQ==');
@$core.Deprecated('Use getExampleRequestDescriptor instead')
const GetExampleRequest$json = const {
  '1': 'GetExampleRequest',
  '2': const [
    const {'1': 'example_uuid', '3': 1, '4': 1, '5': 9, '10': 'exampleUuid'},
  ],
};

/// Descriptor for `GetExampleRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getExampleRequestDescriptor = $convert.base64Decode(
    'ChFHZXRFeGFtcGxlUmVxdWVzdBIhCgxleGFtcGxlX3V1aWQYASABKAlSC2V4YW1wbGVVdWlk');
@$core.Deprecated('Use getExampleResponseDescriptor instead')
const GetExampleResponse$json = const {
  '1': 'GetExampleResponse',
  '2': const [
    const {'1': 'code', '3': 1, '4': 1, '5': 9, '10': 'code'},
  ],
};

/// Descriptor for `GetExampleResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getExampleResponseDescriptor = $convert
    .base64Decode('ChJHZXRFeGFtcGxlUmVzcG9uc2USEgoEY29kZRgBIAEoCVIEY29kZQ==');
