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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Phaser;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;

/**
 * A pool of {@link ArtifactSource ArtifactSources} that can be used as a single source. At least
 * one source must be registered before artifacts can be requested from it.
 *
 * <p>Artifact pooling is required for Flink operators that use the DistributedCache for artifact
 * distribution. This is because distributed caches (along with other runtime context) are scoped to
 * operator lifetimes but the artifact retrieval service must outlive the any remote environments it
 * serves. Remote environments cannot be shared between jobs and are thus job-scoped.
 *
 * <p>Because of the peculiarities of artifact pooling and Flink, this class is packaged with the
 * Flink runner rather than as a core fn-execution utility.
 */
@ThreadSafe
public class ArtifactSourcePool implements ArtifactSource {

  public static ArtifactSourcePool create() {
    return new ArtifactSourcePool();
  }

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<ArtifactSource, Phaser> artifactSources = Maps.newLinkedHashMap();

  private ArtifactSourcePool() {}

  /**
   * Adds a new cache to the pool. When the returned {@link AutoCloseable} is closed, the given
   * cache will be removed from the pool. The call to {@link AutoCloseable#close()} will block until
   * the artifact source is no longer being used.
   */
  public AutoCloseable addToPool(ArtifactSource artifactSource) {
    synchronized (lock) {
      checkState(!artifactSources.containsKey(artifactSource));
      // For this new artifact source, insert a new Phaser with 1 registrant. This party will only
      // be deregistered when the corresponding artifact source is marked as closed. Doing so marks
      // the end of the phase and terminates the phaser.
      artifactSources.put(
          artifactSource,
          new Phaser(1) {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
              // There should only ever be a single phase. Terminate once all registered parties
              // have
              // arrived.
              return true;
            }
          });
      return () -> {
        Phaser phaser;
        synchronized (lock) {
          phaser = artifactSources.remove(artifactSource);
          if (phaser == null) {
            // We've already removed the phaser from the map and attempted to close it.
            return;
          }
          // This indicates we have not yet terminated the phaser. Ensure this is the case.
          checkState(!phaser.isTerminated());
        }
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndDeregister();
      };
    }
  }

  @Override
  public Manifest getManifest() throws IOException {
    SourceHandle handle = getAny();
    try {
      return handle.getSource().getManifest();
    } finally {
      handle.close();
    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    SourceHandle handle = getAny();
    try {
      handle.getSource().getArtifact(name, responseObserver);
    } finally {
      handle.close();
    }
  }

  private SourceHandle getAny() {
    synchronized (lock) {
      checkState(!artifactSources.isEmpty());
      Map.Entry<ArtifactSource, Phaser> entry = artifactSources.entrySet().iterator().next();
      return new SourceHandle(entry.getKey(), entry.getValue());
    }
  }

  private static class SourceHandle {
    private final ArtifactSource artifactSource;
    private final Phaser phaser;

    private boolean isClosed = false;

    SourceHandle(ArtifactSource artifactSource, Phaser phaser) {
      this.artifactSource = artifactSource;
      this.phaser = phaser;
      int registeredPhase = this.phaser.register();
      checkState(registeredPhase >= 0, "Artifact source already closed");
    }

    ArtifactSource getSource() {
      checkState(!phaser.isTerminated());
      return artifactSource;
    }

    void close() {
      if (!isClosed) {
        phaser.arriveAndDeregister();
      }
    }
  }
}
