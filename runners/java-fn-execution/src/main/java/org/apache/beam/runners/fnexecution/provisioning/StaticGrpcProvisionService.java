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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc.ProvisionServiceImplBase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.sdk.fn.server.HeaderAccessor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * A {@link ProvisionServiceImplBase provision service} that returns a static response to all calls.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StaticGrpcProvisionService extends ProvisionServiceGrpc.ProvisionServiceImplBase
    implements FnService {
  public static StaticGrpcProvisionService create(
      ProvisionInfo info, HeaderAccessor headerAccessor) {
    return new StaticGrpcProvisionService(info, headerAccessor);
  }

  private final ProvisionInfo info;

  private final HeaderAccessor headerAccessor;

  private static final Map<String, RunnerApi.Environment> environments = new ConcurrentHashMap<>();

  private StaticGrpcProvisionService(ProvisionInfo info, HeaderAccessor headerAccessor) {
    this.info = info;
    this.headerAccessor = headerAccessor;
  }

  @Override
  public void getProvisionInfo(
      ProvisionApi.GetProvisionInfoRequest request,
      StreamObserver<GetProvisionInfoResponse> responseObserver) {
    if (!environments.containsKey(headerAccessor.getSdkWorkerId())) {
      // TODO(https://github.com/apache/beam/issues/20253): Remove once the JRH is gone.
      responseObserver.onNext(GetProvisionInfoResponse.newBuilder().setInfo(info).build());
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onNext(
        GetProvisionInfoResponse.newBuilder()
            .setInfo(
                info.toBuilder()
                    .addAllDependencies(
                        environments.get(headerAccessor.getSdkWorkerId()).getDependenciesList()))
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void close() throws Exception {}

  public void registerEnvironment(String workerId, RunnerApi.Environment environment) {
    environments.put(workerId, environment);
  }
}
