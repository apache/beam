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
package org.apache.beam.runners.spark;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.core.construction.UnsupportedOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;

/** {@link PTransform} overrides for Flink runner. */
public class SparkTransformOverrides {
  public static List<PTransformOverride> getDefaultOverrides(boolean streaming) {
    ImmutableList.Builder<PTransformOverride> builder = ImmutableList.builder();
    // TODO: [BEAM-5358] Support @RequiresStableInput on Spark runner
    builder.add(
        PTransformOverride.of(
            PTransformMatchers.requiresStableInputParDoMulti(),
            UnsupportedOverrideFactory.withMessage(
                "Spark runner currently doesn't support @RequiresStableInput annotation.")));
    if (!streaming) {
      builder
          .add(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.urnEqualTo(PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                  new SplittableParDoNaiveBounded.OverrideFactory()));
    }
    return builder.build();
  }
}
