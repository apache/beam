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
package org.apache.beam.runners.dataflow.worker.windmill.connectionscache;

import static org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillChannelFactory.inProcessChannel;
import static org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillChannelFactory.remoteChannel;

import com.google.auto.value.AutoOneOf;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.Credentials;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.auth.MoreCallCredentials;

/**
 * Used to create stubs to talk to Streaming Engine. Stubs are either in-process for testing, or
 * remote.
 */
@AutoOneOf(WindmillGrpcStubFactory.Kind.class)
abstract class WindmillGrpcStubFactory {

  static WindmillGrpcStubFactory inProcessStubFactory(String testName) {
    return AutoOneOf_WindmillGrpcStubFactory.inProcess(
        () -> CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel(testName)));
  }

  static WindmillGrpcStubFactory remoteStubFactory(
      int rpcChannelTimeoutSec, Credentials gcpCredentials) {
    return AutoOneOf_WindmillGrpcStubFactory.remote(
        directEndpoint ->
            CloudWindmillServiceV1Alpha1Grpc.newStub(
                    remoteChannel(directEndpoint, rpcChannelTimeoutSec))
                .withCallCredentials(
                    MoreCallCredentials.from(new VendoredCredentialsAdapter(gcpCredentials))));
  }

  abstract Kind getKind();

  abstract Supplier<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub> inProcess();

  abstract Function<
          WindmillServiceAddress, CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub>
      remote();

  enum Kind {
    INPROCESS,
    REMOTE
  }
}
