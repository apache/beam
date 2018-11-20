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

import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;

/**
 * An {@link ArtifactRetrievalService} which has not implemented any methods.
 *
 * <p>For use with an in-process SDK harness.
 */
public class UnsupportedArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {

  public static ArtifactRetrievalService create() {
    return new UnsupportedArtifactRetrievalService();
  }

  private UnsupportedArtifactRetrievalService() {}

  @Override
  public void close() {
    // Do nothing.
  }
}
