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

/**
 * Contains references to components relevant for runners during execution for timers. The
 * referenced PTransform specifies the timer specification while the PCollection specifies the
 * encoding representation.
 */
@AutoValue
public abstract class TimerReference {

  /** Create a timer reference. */
  public static TimerReference of(PipelineNode.PTransformNode transform, String localName) {
    return new AutoValue_TimerReference(transform, localName);
  }

  /** Create a timer reference from a TimerId proto and components. */
  public static TimerReference fromTimerId(
      RunnerApi.ExecutableStagePayload.TimerId timerId, RunnerApi.Components components) {
    String transformId = timerId.getTransformId();
    String localName = timerId.getLocalName();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(transformId);
    return of(PipelineNode.pTransform(transformId, transform), localName);
  }

  /** The PTransform that uses this timer. */
  public abstract PipelineNode.PTransformNode transform();
  /** The local name the referencing PTransform uses to refer to this timer. */
  public abstract String localName();
}
