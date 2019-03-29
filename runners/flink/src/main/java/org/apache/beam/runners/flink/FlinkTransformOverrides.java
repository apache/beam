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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/** {@link PTransform} overrides for Flink runner. */
class FlinkTransformOverrides {
  static List<PTransformOverride> getDefaultOverrides(FlinkPipelineOptions options) {
    ImmutableList.Builder<PTransformOverride> builder = ImmutableList.builder();
    builder
        .add(
            PTransformOverride.of(
                PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.urnEqualTo(PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                options.isStreaming()
                    ? new SplittableParDoViaKeyedWorkItems.OverrideFactory()
                    : new SplittableParDoNaiveBounded.OverrideFactory()));
    if (options.isStreaming()) {
      builder
          .add(
              PTransformOverride.of(
                  PTransformMatchers.writeWithRunnerDeterminedSharding(),
                  new FlinkStreamingPipelineTranslator.StreamingShardedWriteFactory(
                      checkNotNull(options))))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.urnEqualTo(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN),
                  CreateStreamingFlinkView.Factory.INSTANCE));
    }
    return builder.build();
  }
}
