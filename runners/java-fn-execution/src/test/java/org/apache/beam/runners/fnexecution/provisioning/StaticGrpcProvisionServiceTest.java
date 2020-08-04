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
package org.apache.beam.runners.fnexecution.provisioning;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc.ProvisionServiceBlockingStub;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ListValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.NullValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Value;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StaticGrpcProvisionService}. */
@RunWith(JUnit4.class)
public class StaticGrpcProvisionServiceTest {
  @Test
  public void returnsProvisionInfo() throws Exception {
    Struct options =
        Struct.newBuilder()
            .putFields("foo", Value.newBuilder().setBoolValue(true).build())
            .putFields("bar", Value.newBuilder().setNumberValue(2.5).build())
            .putFields(
                "baz",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(
                                Value.newBuilder()
                                    .setStructValue(
                                        Struct.newBuilder()
                                            .putFields(
                                                "spam",
                                                Value.newBuilder()
                                                    .setNullValue(NullValue.NULL_VALUE)
                                                    .build())))
                            .build())
                    .build())
            .build();
    ProvisionInfo info = ProvisionInfo.newBuilder().setPipelineOptions(options).build();
    GrpcFnServer<StaticGrpcProvisionService> server =
        GrpcFnServer.allocatePortAndCreateFor(
            StaticGrpcProvisionService.create(
                info, GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            InProcessServerFactory.create());

    ProvisionServiceBlockingStub stub =
        ProvisionServiceGrpc.newBlockingStub(
            InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl()).build());

    GetProvisionInfoResponse provisionResponse =
        stub.getProvisionInfo(GetProvisionInfoRequest.getDefaultInstance());

    assertThat(provisionResponse.getInfo(), equalTo(info));
  }
}
