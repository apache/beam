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
package org.apache.beam.testinfra.pipelines.dataflow;

import com.google.dataflow.v1beta3.JobsV1Beta3Grpc;
import com.google.dataflow.v1beta3.MetricsV1Beta3Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;

@SuppressWarnings("ForbidNonVendoredGrpcProtobuf")
final class DataflowClientFactory {

  static ManagedChannel channel(DataflowClientFactoryConfiguration configuration) {
    return ManagedChannelBuilder.forTarget(configuration.getDataflowTarget()).build();
  }

  static JobsV1Beta3Grpc.JobsV1Beta3BlockingStub createJobsClient(
      DataflowClientFactoryConfiguration configuration) {
    ManagedChannel channel = channel(configuration);
    return JobsV1Beta3Grpc.newBlockingStub(channel)
        .withCallCredentials(MoreCallCredentials.from(configuration.getCredentials()));
  }

  static MetricsV1Beta3Grpc.MetricsV1Beta3BlockingStub createMetricsClient(
      DataflowClientFactoryConfiguration configuration) {
    ManagedChannel channel = channel(configuration);
    return MetricsV1Beta3Grpc.newBlockingStub(channel)
        .withCallCredentials(MoreCallCredentials.from(configuration.getCredentials()));
  }
}
