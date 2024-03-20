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

import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Channel;

public interface ChannelCachingStubFactory extends WindmillStubFactory {

  /**
   * Remove and close the gRPC channel used to communicate with the given {@link
   * WindmillServiceAddress}.
   *
   * <p>Subsequent calls to {@link
   * WindmillStubFactory#createWindmillServiceStub(WindmillServiceAddress)} will get a stub backed
   * by a new {@link Channel} instance to the {@link WindmillServiceAddress}. Users of stubs backed
   * by the previously vended {@link Channel} will start to receive errors.
   */
  void remove(WindmillServiceAddress windmillServiceAddress);

  /** Shuts down all channels and stubs. */
  void shutdown();
}
