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
package org.apache.beam.fn.harness;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExternalWorkerServiceTest {

  @Test
  public void startWorker() {
    PipelineOptions options = PipelineOptionsFactory.create();
    StartWorkerRequest request = StartWorkerRequest.getDefaultInstance();
    StreamObserver<StartWorkerResponse> responseObserver = mock(StreamObserver.class);
    ExternalWorkerService service = new ExternalWorkerService(options);
    service.startWorker(request, responseObserver);

    verify(responseObserver).onNext(any(StartWorkerResponse.class));
    verify(responseObserver).onCompleted();
  }

  @Test
  public void stopWorker() {
    PipelineOptions options = PipelineOptionsFactory.create();
    StopWorkerRequest request = StopWorkerRequest.getDefaultInstance();
    StreamObserver<StopWorkerResponse> responseObserver = mock(StreamObserver.class);
    ExternalWorkerService service = new ExternalWorkerService(options);
    service.stopWorker(request, responseObserver);

    verify(responseObserver).onNext(any(StopWorkerResponse.class));
    verify(responseObserver).onCompleted();
  }
}
