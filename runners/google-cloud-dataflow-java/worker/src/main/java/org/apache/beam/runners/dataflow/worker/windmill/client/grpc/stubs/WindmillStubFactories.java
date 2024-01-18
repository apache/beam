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

import com.google.auth.Credentials;
import java.util.function.Function;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

@Internal
public final class WindmillStubFactories {

  public static WindmillStubFactory remote(int rpcChannelTimeoutSec, Credentials gcpCredentials) {
    return new RemoteWindmillStubFactory(rpcChannelTimeoutSec, gcpCredentials);
  }

  @VisibleForTesting
  public static WindmillStubFactory inProcess(
      String testName, Function<String, ManagedChannel> channelFactory) {
    return new InProcessWindmillStubFactory(testName, channelFactory);
  }

  @VisibleForTesting
  public static WindmillStubFactory inProcess(String testName) {
    return inProcess(testName, WindmillChannelFactory::inProcessChannel);
  }
}
