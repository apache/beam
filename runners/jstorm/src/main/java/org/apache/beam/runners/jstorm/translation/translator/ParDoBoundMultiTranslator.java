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

import avro.shaded.com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.runtime.DoFnExecutor;
import org.apache.beam.runners.jstorm.translation.runtime.MultiOutputDoFnExecutor;
import org.apache.beam.runners.jstorm.translation.runtime.MultiStatefulDoFnExecutor;
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
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Translates a ParDo.BoundMulti to a Storm {@link DoFnExecutor}.
 */
public class ParDoBoundMultiTranslator<InputT, OutputT>
    extends TransformTranslator.Default<ParDo.MultiOutput<InputT, OutputT>> {

  @Override
  public void translateNode(ParDo.MultiOutput<InputT, OutputT> transform, TranslationContext context) {
    final TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    final TupleTag<InputT> inputTag = (TupleTag<InputT>) userGraphContext.getInputTag();
    PCollection<InputT> input = (PCollection<InputT>) userGraphContext.getInput();

    Map<TupleTag<?>, PValue> allOutputs = Maps.newHashMap(userGraphContext.getOutputs());
    Map<TupleTag<?>, TupleTag<?>> localToExternalTupleTagMap = Maps.newHashMap();
    for (Map.Entry<TupleTag<?>, PValue> entry : allOutputs.entrySet()) {
      Iterator<TupleTag<?>> itr = ((PValueBase) entry.getValue()).expand().keySet().iterator();
      localToExternalTupleTagMap.put(entry.getKey(), itr.next());
    }

    TupleTag<OutputT> mainOutputTag = (TupleTag<OutputT>) userGraphContext.getOutputTag();
    List<TupleTag<?>> sideOutputTags = userGraphContext.getOutputTags();
    sideOutputTags.remove(mainOutputTag);

    Map<TupleTag<?>, PValue> allInputs = Maps.newHashMap(userGraphContext.getInputs());
    for (PCollectionView pCollectionView : transform.getSideInputs()) {
      allInputs.put(userGraphContext.findTupleTag(pCollectionView), pCollectionView);
    }
    String description = describeTransform(
        transform,
        allInputs,
        allOutputs);

    ImmutableMap.Builder<TupleTag, PCollectionView<?>> sideInputTagToView = ImmutableMap.builder();
    for (PCollectionView pCollectionView : transform.getSideInputs()) {
      sideInputTagToView.put(userGraphContext.findTupleTag(pCollectionView), pCollectionView);
    }

    DoFnExecutor executor;
    DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    if (signature.stateDeclarations().size() > 0
        || signature.timerDeclarations().size() > 0) {
      executor = new MultiStatefulDoFnExecutor<>(
          userGraphContext.getStepName(),
          description,
          userGraphContext.getOptions(),
          (DoFn<KV, OutputT>) transform.getFn(),
          (Coder) WindowedValue.getFullCoder(input.getCoder(), input.getWindowingStrategy().getWindowFn().windowCoder()),
          input.getWindowingStrategy(),
          (TupleTag<KV>) inputTag,
          transform.getSideInputs(),
          sideInputTagToView.build(),
          mainOutputTag,
          sideOutputTags,
          localToExternalTupleTagMap);
    } else {
      executor = new MultiOutputDoFnExecutor<>(
          userGraphContext.getStepName(),
          description,
          userGraphContext.getOptions(),
          transform.getFn(),
          WindowedValue.getFullCoder(input.getCoder(), input.getWindowingStrategy().getWindowFn().windowCoder()),
          input.getWindowingStrategy(),
          inputTag,
          transform.getSideInputs(),
          sideInputTagToView.build(),
          mainOutputTag,
          sideOutputTags,
          localToExternalTupleTagMap);
    }

    context.addTransformExecutor(executor, ImmutableList.<PValue>copyOf(transform.getSideInputs()));
  }
}
