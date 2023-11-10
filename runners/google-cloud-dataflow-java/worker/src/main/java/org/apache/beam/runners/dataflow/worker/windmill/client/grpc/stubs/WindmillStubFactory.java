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

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;

import com.google.auth.Credentials;
import com.google.auto.value.AutoOneOf;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.auth.VendoredCredentialsAdapter;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.auth.MoreCallCredentials;

/**
 * Used to create stubs to talk to Streaming Engine. Stubs are either in-process for testing, or
 * remote.
 */
@AutoOneOf(WindmillStubFactory.Kind.class)
public abstract class WindmillStubFactory {

  public static WindmillStubFactory inProcessStubFactory(
      String testName, Function<String, ManagedChannel> channelFactory) {
    return AutoOneOf_WindmillStubFactory.inProcess(
        () -> CloudWindmillServiceV1Alpha1Grpc.newStub(channelFactory.apply(testName)));
  }

  public static WindmillStubFactory inProcessStubFactory(String testName) {
    return AutoOneOf_WindmillStubFactory.inProcess(
        () ->
            CloudWindmillServiceV1Alpha1Grpc.newStub(
                WindmillChannelFactory.inProcessChannel(testName)));
  }

  public static WindmillStubFactory remoteStubFactory(
      int rpcChannelTimeoutSec, Credentials gcpCredentials) {
    return AutoOneOf_WindmillStubFactory.remote(
        directEndpoint ->
            CloudWindmillServiceV1Alpha1Grpc.newStub(
                    remoteChannel(directEndpoint, rpcChannelTimeoutSec))
                .withCallCredentials(
                    MoreCallCredentials.from(new VendoredCredentialsAdapter(gcpCredentials))));
  }

  public abstract Kind getKind();

  public abstract Supplier<CloudWindmillServiceV1Alpha1Stub> inProcess();

  public abstract Function<WindmillServiceAddress, CloudWindmillServiceV1Alpha1Stub> remote();

  public enum Kind {
    IN_PROCESS,
    REMOTE
  }
}
