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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItemCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Interface for creating a runner-specific {@link GroupByKey GroupByKey-like} {@link PTransform}
 * that produces {@link KeyedWorkItem KeyedWorkItems} so that downstream transforms can access state
 * and timers.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class GBKIntoKeyedWorkItems<KeyT, InputT>
    extends PTransform<PCollection<KV<KeyT, InputT>>, PCollection<KeyedWorkItem<KeyT, InputT>>> {
  @Override
  public PCollection<KeyedWorkItem<KeyT, InputT>> apply(PCollection<KV<KeyT, InputT>> input) {
    checkArgument(input.getCoder() instanceof KvCoder,
        "Expected input coder to be KvCoder, but was %s",
        input.getCoder().getClass().getSimpleName());

    KvCoder<KeyT, InputT> kvCoder = (KvCoder<KeyT, InputT>) input.getCoder();
    Coder<KeyedWorkItem<KeyT, InputT>> coder = KeyedWorkItemCoder.of(
        kvCoder.getKeyCoder(), kvCoder.getValueCoder(),
        input.getWindowingStrategy().getWindowFn().windowCoder());
    PCollection<KeyedWorkItem<KeyT, InputT>> collection = PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
    collection.setCoder((Coder) coder);
    return collection;
  }
}
