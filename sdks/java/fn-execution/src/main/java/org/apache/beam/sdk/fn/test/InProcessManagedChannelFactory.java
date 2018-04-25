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
package org.apache.beam.sdk.fn.test;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;

/**
 * A {@link ManagedChannelFactory} that uses in-process channels.
 *
 * <p>The channel builder uses {@link ApiServiceDescriptor#getUrl()} as the unique in-process name.
 */
public class InProcessManagedChannelFactory extends ManagedChannelFactory {

  @Override
  public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
    return InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
  }
}
