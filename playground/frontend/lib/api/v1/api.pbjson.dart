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
import 'dart:convert' as $convert;
import 'dart:core' as $core;
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
    const {'1': 'compilation_status', '3': 2, '4': 1, '5': 14, '6': '.api.v1.Status', '10': 'compilationStatus'},
  ],
};

/// Descriptor for `GetRunOutputResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getRunOutputResponseDescriptor = $convert.base64Decode('ChRHZXRSdW5PdXRwdXRSZXNwb25zZRIWCgZvdXRwdXQYASABKAlSBm91dHB1dBI9ChJjb21waWxhdGlvbl9zdGF0dXMYAiABKA4yDi5hcGkudjEuU3RhdHVzUhFjb21waWxhdGlvblN0YXR1cw==');
