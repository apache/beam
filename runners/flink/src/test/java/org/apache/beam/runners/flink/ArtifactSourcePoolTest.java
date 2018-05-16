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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ArtifactSourcePool}. */
@RunWith(JUnit4.class)
public class ArtifactSourcePoolTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void mustRegisterSourcesFirst() throws Exception {
    ArtifactSourcePool pool = ArtifactSourcePool.create();
    thrown.expect(instanceOf(IllegalStateException.class));
    pool.getManifest();
  }

  @Test
  public void delegatesToPool() throws Exception {
    ArtifactSourcePool pool = ArtifactSourcePool.create();
    ArtifactSource source = mock(ArtifactSource.class);
    Manifest manifest = Manifest.newBuilder().build();
    when(source.getManifest()).thenReturn(manifest);
    AutoCloseable handle = pool.addToPool(source);
    assertThat(pool.getManifest(), sameInstance(manifest));
    handle.close();
  }

  @Test
  public void delegatesAfterClose() throws Exception {
    ArtifactSourcePool pool = ArtifactSourcePool.create();
    ArtifactSource fooSource = mock(ArtifactSource.class);
    ArtifactSource barSource = mock(ArtifactSource.class);
    AutoCloseable fooHandle = pool.addToPool(fooSource);
    AutoCloseable barHandle = pool.addToPool(barSource);
    fooHandle.close();
    // barSource is the only remaining valid source. Ensure this is called.
    pool.getManifest();
    verify(barSource).getManifest();
    barHandle.close();
  }

  @Test
  public void cannotServeAfterClosing() throws Exception {
    ArtifactSourcePool pool = ArtifactSourcePool.create();
    ArtifactSource source = mock(ArtifactSource.class);
    AutoCloseable handle = pool.addToPool(source);
    handle.close();
    thrown.expect(instanceOf(IllegalStateException.class));
    pool.getManifest();
  }

  @Test
  public void waitsUntilConsumersFinished() throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();
    ArtifactSourcePool pool = ArtifactSourcePool.create();
    CompletableFuture<Void> manifestRequested = new CompletableFuture<>();
    CompletableFuture<Void> closeProbablyInvoked = new CompletableFuture<>();
    ArtifactSource source =
        new ArtifactSource() {
          @Override
          public Manifest getManifest() throws IOException {
            manifestRequested.complete(null);
            // Block until close has (probably) been invoked. This means there is a live handle at
            // time that close is invoked and exercises the resource monitor. We cannot test this
            // deterministically because the call to close blocks until completion.
            try {
              closeProbablyInvoked.get();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return null;
          }

          @Override
          public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
            throw new UnsupportedOperationException();
          }
        };
    AutoCloseable handle = pool.addToPool(source);
    CompletableFuture<Manifest> manifestFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return pool.getManifest();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            executor);

    CompletableFuture<Void> closedFuture =
        manifestRequested.handleAsync(
            (v, th) -> {
              try {
                handle.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return null;
            },
            executor);

    manifestRequested.handleAsync(
        (v, th) -> {
          try {
            Thread.sleep(500);
            closeProbablyInvoked.complete(null);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return null;
        },
        executor);

    manifestFuture.get();
    closedFuture.get();

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }
}
