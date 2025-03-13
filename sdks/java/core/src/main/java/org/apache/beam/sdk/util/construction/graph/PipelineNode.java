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
package org.apache.beam.sdk.util.construction.graph;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;

/** A graph node which contains some pipeline element. */
public interface PipelineNode {
  static PTransformNode pTransform(String id, PTransform transform) {
    return new AutoValue_PipelineNode_PTransformNode(id, transform);
  }

  static PCollectionNode pCollection(String id, PCollection collection) {
    return new AutoValue_PipelineNode_PCollectionNode(id, collection);
  }

  String getId();

  /** A {@link PipelineNode} which contains a {@link PCollection}. */
  @AutoValue
  abstract class PCollectionNode implements PipelineNode {
    @Override
    public abstract String getId();

    public abstract PCollection getPCollection();
  }

  /** A {@link PipelineNode} which contains a {@link PTransform}. */
  @AutoValue
  abstract class PTransformNode implements PipelineNode {
    @Override
    public abstract String getId();

    public abstract PTransform getTransform();
  }
}
