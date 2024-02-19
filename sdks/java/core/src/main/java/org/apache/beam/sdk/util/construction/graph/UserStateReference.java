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
import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.UserStateId;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;

/**
 * A reference to user state. This includes the PTransform that references the user state as well as
 * the local name. Both are necessary in order to fully resolve user state.
 */
@AutoValue
public abstract class UserStateReference {

  /** Create a user state reference. */
  public static UserStateReference of(
      PTransformNode transform, String localName, PCollectionNode collection) {
    return new AutoValue_UserStateReference(transform, localName, collection);
  }

  /** Create a user state reference from a UserStateId proto and components. */
  public static UserStateReference fromUserStateId(
      UserStateId userStateId, RunnerApi.Components components) {
    PTransform transform = components.getTransformsOrThrow(userStateId.getTransformId());
    String mainInputCollectionId;
    try {
      mainInputCollectionId =
          transform.getInputsOrThrow(ParDoTranslation.getMainInputName(transform));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return UserStateReference.of(
        PipelineNode.pTransform(userStateId.getTransformId(), transform),
        userStateId.getLocalName(),
        PipelineNode.pCollection(
            mainInputCollectionId, components.getPcollectionsOrThrow(mainInputCollectionId)));
  }

  /** The id of the PTransform that uses this user state. */
  public abstract PTransformNode transform();
  /** The local name the referencing PTransform uses to refer to this user state. */
  public abstract String localName();
  /** The PCollection that represents the input to the PTransform. */
  public abstract PCollectionNode collection();
}
