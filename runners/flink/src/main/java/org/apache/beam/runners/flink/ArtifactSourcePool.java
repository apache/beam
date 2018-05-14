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
import com.google.common.util.concurrent.Monitor;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
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
  private final Map<ArtifactSource, ArtifactSourceLock> artifactSources = Maps.newLinkedHashMap();

  private ArtifactSourcePool() {}

  /**
   * Adds a new cache to the pool. When the returned {@link AutoCloseable} is closed, the given
   * cache will be removed from the pool. The call to {@link AutoCloseable#close()} will block until
   * the artifact source is no longer being used.
   */
  public AutoCloseable addToPool(ArtifactSource artifactSource) {
    synchronized (lock) {
      checkState(!artifactSources.containsKey(artifactSource));
      artifactSources.put(artifactSource, new ArtifactSourceLock());
      return () -> {
        synchronized (lock) {
          ArtifactSourceLock innerLock = artifactSources.remove(artifactSource);
          checkState(innerLock != null);
          innerLock.close();
        }
      };
    }
  }

  @Override
  public Manifest getManifest() throws IOException {
    ArtifactSource source;
    SourceHandle sourceHandle;
    synchronized (lock) {
      checkState(!artifactSources.isEmpty());
      Map.Entry<ArtifactSource, ArtifactSourceLock> entry =
          artifactSources.entrySet().iterator().next();
      source = entry.getKey();
      sourceHandle = entry.getValue().open();
    }
    try {
      return source.getManifest();
    } finally {
      sourceHandle.close();
    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    ArtifactSource source;
    SourceHandle sourceHandle;
    synchronized (lock) {
      checkState(!artifactSources.isEmpty());
      Map.Entry<ArtifactSource, ArtifactSourceLock> entry =
          artifactSources.entrySet().iterator().next();
      source = entry.getKey();
      sourceHandle = entry.getValue().open();
    }
    try {
      source.getArtifact(name, responseObserver);
    } finally {
      sourceHandle.close();
    }
  }

  /** Manages the state of a composed artifact source. */
  private static class ArtifactSourceLock {
    private final Monitor monitor = new Monitor();
    private final Monitor.Guard isClosed =
        new Monitor.Guard(monitor) {
          @Override
          public boolean isSatisfied() {
            return closeRequested && refCount == 0;
          }
        };

    private boolean closeRequested = false;
    private int refCount = 0;

    ArtifactSourceLock() {}

    SourceHandle open() {
      monitor.enter();
      try {
        checkState(!closeRequested);
        refCount++;
      } finally {
        monitor.leave();
      }

      return new SourceHandle() {
        boolean closed = false;

        @Override
        public void close() {
          monitor.enter();
          try {
            if (!closed) {
              checkState(refCount > 0);
              closed = true;
              refCount--;
            }
          } finally {
            monitor.leave();
          }
        }
      };
    }

    /** Marks the current source as closed and waits for all consumers to finish. */
    void close() {
      monitor.enter();
      try {
        closeRequested = true;
      } finally {
        monitor.leave();
      }

      try {
        monitor.enterWhen(isClosed);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      monitor.leave();
    }
  }

  private interface SourceHandle {
    void close();
  }
}
