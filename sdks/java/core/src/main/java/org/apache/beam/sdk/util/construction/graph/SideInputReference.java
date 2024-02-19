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
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;

/**
 * A reference to a side input. This includes the PTransform that references the side input as well
 * as the PCollection referenced. Both are necessary in order to fully resolve a view.
 */
@AutoValue
public abstract class SideInputReference {

  /** Create a side input reference. */
  public static SideInputReference of(
      PTransformNode transform, String localName, PCollectionNode collection) {
    return new AutoValue_SideInputReference(transform, localName, collection);
  }

  /** Create a side input reference from a SideInputId proto and components. */
  public static SideInputReference fromSideInputId(
      SideInputId sideInputId, RunnerApi.Components components) {
    String transformId = sideInputId.getTransformId();
    String localName = sideInputId.getLocalName();
    String collectionId = components.getTransformsOrThrow(transformId).getInputsOrThrow(localName);
    PTransform transform = components.getTransformsOrThrow(transformId);
    PCollection collection = components.getPcollectionsOrThrow(collectionId);
    return SideInputReference.of(
        PipelineNode.pTransform(transformId, transform),
        localName,
        PipelineNode.pCollection(collectionId, collection));
  }

  /** The PTransform that uses this side input. */
  public abstract PTransformNode transform();
  /** The local name the referencing PTransform uses to refer to this side input. */
  public abstract String localName();
  /** The PCollection that backs this side input. */
  public abstract PCollectionNode collection();

  @Override
  public final String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Transform", transform().toString())
        .add("PCollection", collection().toString())
        .toString();
  }
}
