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
package org.apache.beam.runners.jstorm.translation.translator;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.runtime.GroupByWindowExecutor;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

public class GroupByKeyTranslator<K, V> extends TransformTranslator.Default<GroupByKey<K, V>> {
  // information of transform
  protected PCollection<KV<K, V>> input;
  protected PCollection<KV<K, Iterable<V>>> output;
  protected List<TupleTag<?>> inputTags;
  protected TupleTag<KV<K, Iterable<V>>> mainOutputTag;
  protected List<TupleTag<?>> sideOutputTags;
  protected List<PCollectionView<?>> sideInputs;
  protected WindowingStrategy<?, ?> windowingStrategy;

  @Override
  public void translateNode(GroupByKey<K, V> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    String description =
        describeTransform(transform, userGraphContext.getInputs(), userGraphContext.getOutputs());

    input = (PCollection<KV<K, V>>) userGraphContext.getInput();
    output = (PCollection<KV<K, Iterable<V>>>) userGraphContext.getOutput();

    inputTags = userGraphContext.getInputTags();
    mainOutputTag = (TupleTag<KV<K, Iterable<V>>>) userGraphContext.getOutputTag();
    sideOutputTags = Lists.newArrayList();

    sideInputs = Collections.<PCollectionView<?>>emptyList();
    windowingStrategy = input.getWindowingStrategy();

    GroupByWindowExecutor<K, V> groupByWindowExecutor = new GroupByWindowExecutor<>(
        userGraphContext.getStepName(),
        description,
        context,
        context.getUserGraphContext().getOptions(),
        windowingStrategy,
        mainOutputTag,
        sideOutputTags);
    context.addTransformExecutor(groupByWindowExecutor);
  }
}
