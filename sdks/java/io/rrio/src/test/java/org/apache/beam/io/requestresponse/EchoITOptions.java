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
package org.apache.beam.io.requestresponse;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.testinfra.mockapis.echo.v1.EchoServiceGrpc;

/**
 * Shared options for running integration tests on a deployed {@link EchoServiceGrpc}. See <a
 * href="https://github.com/apache/beam/tree/master/.test-infra/mock-apis#integration">https://github.com/apache/beam/tree/master/.test-infra/mock-apis#integration</a>
 * for details on how to acquire values required by {@link EchoITOptions}.
 *
 * <p>To provide these values to your integration tests:
 *
 * <pre>
 *   ./gradlew :sdks:java:io:rrio:integrationTest -DintegrationTestPipelineOptions='[
 *      "--grpcEndpointAddress=",
 *      "--httpEndpointAddress="
 *   ]'
 * </pre>
 */
public interface EchoITOptions extends PipelineOptions {
  String GRPC_ENDPOINT_ADDRESS_NAME = "grpcEndpointAddress";
  String HTTP_ENDPOINT_ADDRESS_NAME = "httpEndpointAddress";

  @Description("The gRPC address of the Echo API endpoint, typically of the form <host>:<port>.")
  String getGrpcEndpointAddress();

  void setGrpcEndpointAddress(String value);

  @Description("The HTTP address of the Echo API endpoint; must being with http(s)://")
  String getHttpEndpointAddress();

  void setHttpEndpointAddress(String value);

  @Description("The ID for an allocated quota that should never exceed.")
  @Default.String("echo-should-never-exceed-quota")
  String getNeverExceedQuotaId();

  void setNeverExceedQuotaId(String value);

  @Description("The ID for an allocated quota that should exceed.")
  @Default.String("echo-should-exceed-quota")
  String getShouldExceedQuotaId();

  void setShouldExceedQuotaId(String value);
}
