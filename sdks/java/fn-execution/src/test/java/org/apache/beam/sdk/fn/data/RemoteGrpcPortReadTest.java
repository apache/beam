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
package org.apache.beam.sdk.fn.data;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.Endpoints.AuthenticationSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RemoteGrpcPortRead}. */
@RunWith(JUnit4.class)
public class RemoteGrpcPortReadTest {
  @Test
  public void getPortSucceeds() {
    RemoteGrpcPort port =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(
                ApiServiceDescriptor.newBuilder()
                    .setUrl("foo")
                    .setAuthentication(AuthenticationSpec.getDefaultInstance())
                    .build())
            .build();

    RemoteGrpcPortRead read = RemoteGrpcPortRead.readFromPort(port, "myPort");
    assertThat(read.getPort(), equalTo(port));
  }

  @Test
  public void toFromPTransform() throws InvalidProtocolBufferException {
    RemoteGrpcPort port =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(
                ApiServiceDescriptor.newBuilder()
                    .setUrl("foo")
                    .setAuthentication(AuthenticationSpec.getDefaultInstance())
                    .build())
            .build();

    RemoteGrpcPortRead read = RemoteGrpcPortRead.readFromPort(port, "myPort");
    PTransform ptransform = PTransform.parseFrom(read.toPTransform().toByteArray());
    RemoteGrpcPortRead serDeRead = RemoteGrpcPortRead.fromPTransform(ptransform);

    assertThat(serDeRead, equalTo(read));
    assertThat(serDeRead.getPort(), equalTo(read.getPort()));
    assertThat(serDeRead.toPTransform(), equalTo(ptransform));
  }
}
