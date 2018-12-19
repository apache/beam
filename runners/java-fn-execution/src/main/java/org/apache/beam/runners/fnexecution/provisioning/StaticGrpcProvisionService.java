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

import static org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc.ProvisionServiceImplBase;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;

/**
 * A {@link ProvisionServiceImplBase provision service} that returns a static response to all calls.
 */
public class StaticGrpcProvisionService extends ProvisionServiceGrpc.ProvisionServiceImplBase
    implements FnService {
  public static StaticGrpcProvisionService create(ProvisionInfo info) {
    return new StaticGrpcProvisionService(info);
  }

  private final ProvisionInfo info;

  private StaticGrpcProvisionService(ProvisionInfo info) {
    this.info = info;
  }

  @Override
  public void getProvisionInfo(
      ProvisionApi.GetProvisionInfoRequest request,
      StreamObserver<GetProvisionInfoResponse> responseObserver) {
    responseObserver.onNext(GetProvisionInfoResponse.newBuilder().setInfo(info).build());
    responseObserver.onCompleted();
  }

  @Override
  public void close() throws Exception {}
}
