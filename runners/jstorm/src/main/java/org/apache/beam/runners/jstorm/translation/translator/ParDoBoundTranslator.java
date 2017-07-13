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

import avro.shaded.com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.runtime.DoFnExecutor;
import org.apache.beam.runners.jstorm.translation.runtime.StatefulDoFnExecutor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates a ParDo.Bound to a Storm {@link DoFnExecutor}.
 */
public class ParDoBoundTranslator<InputT, OutputT>
    extends TransformTranslator.Default<ParDo.SingleOutput<InputT, OutputT>> {

  private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundTranslator.class);

  @Override
  public void translateNode(
      ParDo.SingleOutput<InputT, OutputT> transform, TranslationContext context) {
    final TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    final TupleTag<?> inputTag = userGraphContext.getInputTag();
    PCollection<InputT> input = (PCollection<InputT>) userGraphContext.getInput();

    TupleTag<OutputT> mainOutputTag = (TupleTag<OutputT>) userGraphContext.getOutputTag();
    List<TupleTag<?>> sideOutputTags = Lists.newArrayList();

    Map<TupleTag<?>, PValue> allInputs =
        avro.shaded.com.google.common.collect.Maps.newHashMap(userGraphContext.getInputs());
    for (PCollectionView pCollectionView : transform.getSideInputs()) {
      allInputs.put(userGraphContext.findTupleTag(pCollectionView), pCollectionView);
    }
    String description = describeTransform(
        transform,
        allInputs,
        userGraphContext.getOutputs());

    ImmutableMap.Builder<TupleTag, PCollectionView<?>> sideInputTagToView = ImmutableMap.builder();
    for (PCollectionView pCollectionView : transform.getSideInputs()) {
      sideInputTagToView.put(userGraphContext.findTupleTag(pCollectionView), pCollectionView);
    }

    DoFnExecutor executor;
    DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    if (signature.stateDeclarations().size() > 0
        || signature.timerDeclarations().size() > 0) {
      executor = new StatefulDoFnExecutor<>(
          userGraphContext.getStepName(),
          description,
          userGraphContext.getOptions(),
          (DoFn<KV, OutputT>) transform.getFn(),
          (Coder) WindowedValue.getFullCoder(
              input.getCoder(), input.getWindowingStrategy().getWindowFn().windowCoder()),
          input.getWindowingStrategy(),
          (TupleTag<KV>) inputTag,
          transform.getSideInputs(),
          sideInputTagToView.build(),
          mainOutputTag,
          sideOutputTags);
    } else {
      executor = new DoFnExecutor<>(
          userGraphContext.getStepName(),
          description,
          userGraphContext.getOptions(),
          transform.getFn(),
          WindowedValue.getFullCoder(
              input.getCoder(), input.getWindowingStrategy().getWindowFn().windowCoder()),
          input.getWindowingStrategy(),
          (TupleTag<InputT>) inputTag,
          transform.getSideInputs(),
          sideInputTagToView.build(),
          mainOutputTag,
          sideOutputTags);
    }

    context.addTransformExecutor(executor, ImmutableList.<PValue>copyOf(transform.getSideInputs()));
  }
}
