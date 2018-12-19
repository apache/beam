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
package org.apache.beam.runners.direct.portable.artifact;

import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UnsupportedArtifactRetrievalService}. */
@RunWith(JUnit4.class)
public class UnsupportedArtifactRetrievalServiceTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private GrpcFnServer<ArtifactRetrievalService> server;
  private ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub stub;

  @Before
  public void setUp() throws Exception {
    server =
        GrpcFnServer.allocatePortAndCreateFor(
            UnsupportedArtifactRetrievalService.create(), InProcessServerFactory.create());
    stub =
        ArtifactRetrievalServiceGrpc.newStub(
            InProcessManagedChannelFactory.create()
                .forDescriptor(server.getApiServiceDescriptor()));
  }

  @Test
  public void getArtifactThrows() throws Exception {
    SynchronousQueue<Optional<Throwable>> thrown = new SynchronousQueue<>();
    stub.getArtifact(
        GetArtifactRequest.newBuilder().setName("foo").build(),
        new StreamObserver<ArtifactChunk>() {
          @Override
          public void onNext(ArtifactChunk value) {
            try {
              thrown.put(Optional.empty());
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }

          @Override
          public void onError(Throwable t) {
            try {
              thrown.put(Optional.of(t));
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }

          @Override
          public void onCompleted() {
            try {
              thrown.put(Optional.empty());
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }
        });
    Throwable wasThrown =
        thrown
            .take()
            .orElseThrow(
                () ->
                    new AssertionError(
                        String.format(
                            "The %s should respond to all calls with an error",
                            UnsupportedArtifactRetrievalServiceTest.class.getSimpleName())));
  }

  @Test
  public void getManifestThrows() throws Exception {
    SynchronousQueue<Optional<Throwable>> thrown = new SynchronousQueue<>();
    stub.getManifest(
        GetManifestRequest.newBuilder().build(),
        new StreamObserver<GetManifestResponse>() {
          @Override
          public void onNext(GetManifestResponse value) {
            try {
              thrown.put(Optional.empty());
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }

          @Override
          public void onError(Throwable t) {
            try {
              thrown.put(Optional.of(t));
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }

          @Override
          public void onCompleted() {
            try {
              thrown.put(Optional.empty());
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }
        });
    Throwable wasThrown =
        thrown
            .take()
            .orElseThrow(
                () ->
                    new AssertionError(
                        String.format(
                            "The %s should respond to all calls with an error",
                            UnsupportedArtifactRetrievalServiceTest.class.getSimpleName())));
  }

  @Test
  public void closeCompletes() throws Exception {
    server.getService().close();
  }
}
