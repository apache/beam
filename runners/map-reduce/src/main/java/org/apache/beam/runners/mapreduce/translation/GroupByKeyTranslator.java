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
package org.apache.beam.runners.mapreduce.translation;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Translates a {@link GroupByKey} to {@link Operation Operations}.
 */
class GroupByKeyTranslator<K, V> extends TransformTranslator.Default<GroupByKey<K, V>> {
  @Override
  public void translateNode(GroupByKey<K, V> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();

    PCollection<?> inPCollection = (PCollection<?>) userGraphContext.getInput();
    WindowingStrategy<?, ?> windowingStrategy = inPCollection.getWindowingStrategy();
    Coder<?> inCoder = inPCollection.getCoder();

    GroupByKeyOperation<K, V> groupByKeyOperation =
        new GroupByKeyOperation<>(windowingStrategy, (KvCoder<K, V>) inCoder);
    context.addInitStep(
        Graphs.Step.of(userGraphContext.getStepName(), groupByKeyOperation),
        userGraphContext.getInputTags(),
        userGraphContext.getOutputTags());
  }
}
