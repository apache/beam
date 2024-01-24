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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;

/**
 * Creates in process stubs to talk to Streaming Engine. Only recommended to be used for testing.
 */
final class InProcessWindmillStubFactory implements WindmillStubFactory {
  private final String testName;
  private final Function<String, ManagedChannel> channelFactory;

  InProcessWindmillStubFactory(String testName, Function<String, ManagedChannel> channelFactory) {
    this.testName = testName;
    this.channelFactory = channelFactory;
  }

  @Override
  public CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub
      createWindmillServiceStub(WindmillServiceAddress serviceAddress) {
    return CloudWindmillServiceV1Alpha1Grpc.newStub(channelFactory.apply(testName));
  }

  @Override
  public CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub
      createWindmillMetadataServiceStub(WindmillServiceAddress serviceAddress) {
    return CloudWindmillMetadataServiceV1Alpha1Grpc.newStub(channelFactory.apply(testName));
  }
}
