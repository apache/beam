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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * {@link PTransform} overrides for Flink runner.
 */
public class FlinkTransformOverrides {
  public static List<PTransformOverride> getDefaultOverrides(boolean streaming) {
    if (streaming) {
      return ImmutableList.<PTransformOverride>builder()
          .add(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDo(),
                  new FlinkStreamingPipelineTranslator.SplittableParDoOverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.urnEqualTo(
                      PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                  new SplittableParDoViaKeyedWorkItems.OverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.urnEqualTo(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN),
                  new CreateStreamingFlinkView.Factory()))
          .build();
    } else {
      return ImmutableList.of();
    }
  }
}
