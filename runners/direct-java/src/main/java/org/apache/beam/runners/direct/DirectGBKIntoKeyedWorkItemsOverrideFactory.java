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
package org.apache.beam.runners.direct;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDo.GBKIntoKeyedWorkItems;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * Provides an implementation of {@link SplittableParDo.GBKIntoKeyedWorkItems} for the Direct
 * Runner.
 */
class DirectGBKIntoKeyedWorkItemsOverrideFactory<KeyT, InputT>
    implements PTransformOverrideFactory<
        PCollection<KV<KeyT, InputT>>, PCollection<KeyedWorkItem<KeyT, InputT>>,
        GBKIntoKeyedWorkItems<KeyT, InputT>> {
  @Override
  public PTransform<PCollection<KV<KeyT, InputT>>, PCollection<KeyedWorkItem<KeyT, InputT>>>
      getReplacementTransform(GBKIntoKeyedWorkItems<KeyT, InputT> transform) {
    return new DirectGroupByKey.DirectGroupByKeyOnly<>();
  }

  @Override
  public PCollection<KV<KeyT, InputT>> getInput(
      List<TaggedPValue> inputs, Pipeline p) {
    return (PCollection<KV<KeyT, InputT>>) Iterables.getOnlyElement(inputs).getValue();
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, PCollection<KeyedWorkItem<KeyT, InputT>> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }
}
