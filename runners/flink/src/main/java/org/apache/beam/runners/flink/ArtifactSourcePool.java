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

import io.grpc.stub.StreamObserver;
import java.io.IOException;
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

  private ArtifactSourcePool() {}

  /**
   * Adds a new cache to the pool. When the returned {@link AutoCloseable} is closed, the given
   * cache will be removed from the pool.
   */
  public AutoCloseable addToPool(ArtifactSource artifactSource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Manifest getManifest() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    throw new UnsupportedOperationException();
  }
}
