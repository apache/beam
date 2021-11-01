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
    const {'1': 'STATUS_COMPILE_ERROR', '2': 4},
    const {'1': 'STATUS_RUN_TIMEOUT', '2': 5},
  ],
};

/// Descriptor for `Status`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List statusDescriptor = $convert.base64Decode('CgZTdGF0dXMSFgoSU1RBVFVTX1VOU1BFQ0lGSUVEEAASFAoQU1RBVFVTX0VYRUNVVElORxABEhMKD1NUQVRVU19GSU5JU0hFRBACEhAKDFNUQVRVU19FUlJPUhADEhgKFFNUQVRVU19DT01QSUxFX0VSUk9SEAQSFgoSU1RBVFVTX1JVTl9USU1FT1VUEAU=');
@$core.Deprecated('Use exampleTypeDescriptor instead')
const ExampleType$json = const {
  '1': 'ExampleType',
  '2': const [
    const {'1': 'EXAMPLE_TYPE_DEFAULT', '2': 0},
    const {'1': 'EXAMPLE_TYPE_KATA', '2': 1},
    const {'1': 'EXAMPLE_TYPE_UNIT_TEST', '2': 2},
  ],
};

/// Descriptor for `ExampleType`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List exampleTypeDescriptor = $convert.base64Decode('CgtFeGFtcGxlVHlwZRIYChRFWEFNUExFX1RZUEVfREVGQVVMVBAAEhUKEUVYQU1QTEVfVFlQRV9LQVRBEAESGgoWRVhBTVBMRV9UWVBFX1VOSVRfVEVTVBAC');
@$core.Deprecated('Use runCodeRequestDescriptor instead')
const RunCodeRequest$json = const {
  '1': 'RunCodeRequest',
  '2': const [
    const {'1': 'code', '3': 1, '4': 1, '5': 9, '10': 'code'},
    const {'1': 'sdk', '3': 2, '4': 1, '5': 14, '6': '.api.v1.Sdk', '10': 'sdk'},
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
    const {'1': 'compilation_status', '3': 2, '4': 1, '5': 14, '6': '.api.v1.Status', '10': 'compilationStatus'},
  ],
};

/// Descriptor for `GetRunOutputResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getRunOutputResponseDescriptor = $convert.base64Decode('ChRHZXRSdW5PdXRwdXRSZXNwb25zZRIWCgZvdXRwdXQYASABKAlSBm91dHB1dBI9ChJjb21waWxhdGlvbl9zdGF0dXMYAiABKA4yDi5hcGkudjEuU3RhdHVzUhFjb21waWxhdGlvblN0YXR1cw==');
@$core.Deprecated('Use getListOfExamplesRequestDescriptor instead')
const GetListOfExamplesRequest$json = const {
  '1': 'GetListOfExamplesRequest',
  '2': const [
    const {'1': 'sdk', '3': 1, '4': 1, '5': 14, '6': '.api.v1.Sdk', '10': 'sdk'},
    const {'1': 'category', '3': 2, '4': 1, '5': 9, '10': 'category'},
  ],
};

/// Descriptor for `GetListOfExamplesRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getListOfExamplesRequestDescriptor = $convert.base64Decode('ChhHZXRMaXN0T2ZFeGFtcGxlc1JlcXVlc3QSHQoDc2RrGAEgASgOMgsuYXBpLnYxLlNka1IDc2RrEhoKCGNhdGVnb3J5GAIgASgJUghjYXRlZ29yeQ==');
@$core.Deprecated('Use exampleDescriptor instead')
const Example$json = const {
  '1': 'Example',
  '2': const [
    const {'1': 'example_uuid', '3': 1, '4': 1, '5': 9, '10': 'exampleUuid'},
    const {'1': 'name', '3': 2, '4': 1, '5': 9, '10': 'name'},
    const {'1': 'description', '3': 3, '4': 1, '5': 9, '10': 'description'},
    const {'1': 'type', '3': 4, '4': 1, '5': 14, '6': '.api.v1.ExampleType', '10': 'type'},
  ],
};

/// Descriptor for `Example`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List exampleDescriptor = $convert.base64Decode('CgdFeGFtcGxlEiEKDGV4YW1wbGVfdXVpZBgBIAEoCVILZXhhbXBsZVV1aWQSEgoEbmFtZRgCIAEoCVIEbmFtZRIgCgtkZXNjcmlwdGlvbhgDIAEoCVILZGVzY3JpcHRpb24SJwoEdHlwZRgEIAEoDjITLmFwaS52MS5FeGFtcGxlVHlwZVIEdHlwZQ==');
@$core.Deprecated('Use categoriesDescriptor instead')
const Categories$json = const {
  '1': 'Categories',
  '2': const [
    const {'1': 'sdk', '3': 1, '4': 1, '5': 14, '6': '.api.v1.Sdk', '10': 'sdk'},
    const {'1': 'categories', '3': 2, '4': 3, '5': 11, '6': '.api.v1.Categories.Category', '10': 'categories'},
  ],
  '3': const [Categories_Category$json],
};

@$core.Deprecated('Use categoriesDescriptor instead')
const Categories_Category$json = const {
  '1': 'Category',
  '2': const [
    const {'1': 'category_name', '3': 1, '4': 1, '5': 9, '10': 'categoryName'},
    const {'1': 'examples', '3': 2, '4': 3, '5': 11, '6': '.api.v1.Example', '10': 'examples'},
  ],
};

/// Descriptor for `Categories`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List categoriesDescriptor = $convert.base64Decode('CgpDYXRlZ29yaWVzEh0KA3NkaxgBIAEoDjILLmFwaS52MS5TZGtSA3NkaxI7CgpjYXRlZ29yaWVzGAIgAygLMhsuYXBpLnYxLkNhdGVnb3JpZXMuQ2F0ZWdvcnlSCmNhdGVnb3JpZXMaXAoIQ2F0ZWdvcnkSIwoNY2F0ZWdvcnlfbmFtZRgBIAEoCVIMY2F0ZWdvcnlOYW1lEisKCGV4YW1wbGVzGAIgAygLMg8uYXBpLnYxLkV4YW1wbGVSCGV4YW1wbGVz');
@$core.Deprecated('Use getListOfExamplesResponseDescriptor instead')
const GetListOfExamplesResponse$json = const {
  '1': 'GetListOfExamplesResponse',
  '2': const [
    const {'1': 'sdk_examples', '3': 1, '4': 3, '5': 11, '6': '.api.v1.Categories', '10': 'sdkExamples'},
  ],
};

/// Descriptor for `GetListOfExamplesResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getListOfExamplesResponseDescriptor = $convert.base64Decode('ChlHZXRMaXN0T2ZFeGFtcGxlc1Jlc3BvbnNlEjUKDHNka19leGFtcGxlcxgBIAMoCzISLmFwaS52MS5DYXRlZ29yaWVzUgtzZGtFeGFtcGxlcw==');
@$core.Deprecated('Use getExampleRequestDescriptor instead')
const GetExampleRequest$json = const {
  '1': 'GetExampleRequest',
  '2': const [
    const {'1': 'example_uuid', '3': 1, '4': 1, '5': 9, '10': 'exampleUuid'},
  ],
};

/// Descriptor for `GetExampleRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getExampleRequestDescriptor = $convert.base64Decode('ChFHZXRFeGFtcGxlUmVxdWVzdBIhCgxleGFtcGxlX3V1aWQYASABKAlSC2V4YW1wbGVVdWlk');
@$core.Deprecated('Use getExampleResponseDescriptor instead')
const GetExampleResponse$json = const {
  '1': 'GetExampleResponse',
  '2': const [
    const {'1': 'code', '3': 1, '4': 1, '5': 9, '10': 'code'},
  ],
};

/// Descriptor for `GetExampleResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getExampleResponseDescriptor = $convert.base64Decode('ChJHZXRFeGFtcGxlUmVzcG9uc2USEgoEY29kZRgBIAEoCVIEY29kZQ==');
