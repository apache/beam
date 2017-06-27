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

import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A {@link PTransformOverrideFactory} that provides overrides for applications of a {@link ParDo}
 * in the direct runner. Currently overrides applications of <a
 * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a>.
 */
class ParDoMultiOverrideFactory<InputT, OutputT>
    implements PTransformOverrideFactory<
        PCollection<? extends InputT>, PCollectionTuple, MultiOutput<InputT, OutputT>> {
  @Override
  public PTransformReplacement<PCollection<? extends InputT>, PCollectionTuple>
      getReplacementTransform(
          AppliedPTransform<
                  PCollection<? extends InputT>, PCollectionTuple, MultiOutput<InputT, OutputT>>
              transform) {
    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        getReplacementTransform(transform.getTransform()));
  }

  @SuppressWarnings("unchecked")
  private PTransform<PCollection<? extends InputT>, PCollectionTuple> getReplacementTransform(
      MultiOutput<InputT, OutputT> transform) {

    DoFn<InputT, OutputT> fn = transform.getFn();
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
    if (signature.processElement().isSplittable()) {
      return (PTransform) SplittableParDo.forJavaParDo(transform);
    } else if (signature.stateDeclarations().size() > 0
        || signature.timerDeclarations().size() > 0) {

      // Based on the fact that the signature is stateful, DoFnSignatures ensures
      // that it is also keyed
      return new GbkThenStatefulParDo(
          fn,
          transform.getMainOutputTag(),
          transform.getAdditionalOutputTags(),
          transform.getSideInputs());
    } else {
      return transform;
    }
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PCollectionTuple newOutput) {
    return ReplacementOutputs.tagged(outputs, newOutput);
  }

  static class GbkThenStatefulParDo<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollectionTuple> {
    private final transient DoFn<KV<K, InputT>, OutputT> doFn;
    private final TupleTagList additionalOutputTags;
    private final TupleTag<OutputT> mainOutputTag;
    private final List<PCollectionView<?>> sideInputs;

    public GbkThenStatefulParDo(
        DoFn<KV<K, InputT>, OutputT> doFn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        List<PCollectionView<?>> sideInputs) {
      this.doFn = doFn;
      this.additionalOutputTags = additionalOutputTags;
      this.mainOutputTag = mainOutputTag;
      this.sideInputs = sideInputs;
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<K, InputT>> input) {

      WindowingStrategy<?, ?> inputWindowingStrategy = input.getWindowingStrategy();

      // A KvCoder is required since this goes through GBK. Further, WindowedValueCoder
      // is not registered by default, so we explicitly set the relevant coders.
      checkState(
          input.getCoder() instanceof KvCoder,
          "Input to a %s using state requires a %s, but the coder was %s",
          ParDo.class.getSimpleName(),
          KvCoder.class.getSimpleName(),
          input.getCoder());
      KvCoder<K, InputT> kvCoder = (KvCoder<K, InputT>) input.getCoder();
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends BoundedWindow> windowCoder =
          inputWindowingStrategy.getWindowFn().windowCoder();

      PCollection<KeyedWorkItem<K, KV<K, InputT>>> adjustedInput =
          input
              // Stash the original timestamps, etc, for when it is fed to the user's DoFn
              .apply("Reify timestamps", ParDo.of(new ReifyWindowedValueFn<K, InputT>()))
              .setCoder(KvCoder.of(keyCoder, WindowedValue.getFullCoder(kvCoder, windowCoder)))

              // We are going to GBK to gather keys and windows but otherwise do not want
              // to alter the flow of data. This entails:
              //  - trigger as fast as possible
              //  - maintain the full timestamps of elements
              //  - ensure this GBK holds to the minimum of those timestamps (via TimestampCombiner)
              //  - discard past panes as it is "just a stream" of elements
              .apply(
                  Window.<KV<K, WindowedValue<KV<K, InputT>>>>configure()
                      .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                      .discardingFiredPanes()
                      .withAllowedLateness(inputWindowingStrategy.getAllowedLateness())
                      .withTimestampCombiner(TimestampCombiner.EARLIEST))

              // A full GBK to group by key _and_ window
              .apply("Group by key", GroupByKey.<K, WindowedValue<KV<K, InputT>>>create())

              // Adapt to KeyedWorkItem; that is how this runner delivers timers
              .apply("To KeyedWorkItem", ParDo.of(new ToKeyedWorkItem<K, InputT>()))
              .setCoder(KeyedWorkItemCoder.of(keyCoder, kvCoder, windowCoder))

              // Because of the intervening GBK, we may have abused the windowing strategy
              // of the input, which should be transferred to the output in a straightforward manner
              // according to what ParDo already does.
              .setWindowingStrategyInternal(inputWindowingStrategy);

      PCollectionTuple outputs =
          adjustedInput
              // Explode the resulting iterable into elements that are exactly the ones from
              // the input
              .apply(
              "Stateful ParDo",
              new StatefulParDo<>(doFn, mainOutputTag, additionalOutputTags, sideInputs));

      return outputs;
    }
  }

  static final String DIRECT_STATEFUL_PAR_DO_URN =
      "urn:beam:directrunner:transforms:stateful_pardo:v1";

  static class StatefulParDo<K, InputT, OutputT>
      extends PTransformTranslation.RawPTransform<
          PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple> {
    private final transient DoFn<KV<K, InputT>, OutputT> doFn;
    private final TupleTagList additionalOutputTags;
    private final TupleTag<OutputT> mainOutputTag;
    private final List<PCollectionView<?>> sideInputs;

    public StatefulParDo(
        DoFn<KV<K, InputT>, OutputT> doFn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        List<PCollectionView<?>> sideInputs) {
      this.doFn = doFn;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
      this.sideInputs = sideInputs;
    }

    public DoFn<KV<K, InputT>, OutputT> getDoFn() {
      return doFn;
    }

    public TupleTag<OutputT> getMainOutputTag() {
      return mainOutputTag;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    public TupleTagList getAdditionalOutputTags() {
      return additionalOutputTags;
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    @Override
    public PCollectionTuple expand(PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>> input) {

      PCollectionTuple outputs =
          PCollectionTuple.ofPrimitiveOutputsInternal(
              input.getPipeline(),
              TupleTagList.of(getMainOutputTag()).and(getAdditionalOutputTags().getAll()),
              input.getWindowingStrategy(),
              input.isBounded());

      return outputs;
    }

    @Override
    public String getUrn() {
      return DIRECT_STATEFUL_PAR_DO_URN;
    }
  }

  /**
   * A distinguished key-preserving {@link DoFn}.
   *
   * <p>This wraps the {@link GroupByKey} output in a {@link KeyedWorkItem} to be able to deliver
   * timers. It also explodes them into single {@link KV KVs} since this is what the user's {@link
   * DoFn} needs to process anyhow.
   */
  static class ReifyWindowedValueFn<K, V> extends DoFn<KV<K, V>, KV<K, WindowedValue<KV<K, V>>>> {
    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      c.output(
          KV.of(
              c.element().getKey(),
              WindowedValue.of(c.element(), c.timestamp(), window, c.pane())));
    }
  }

  /**
   * A runner-specific primitive that is just a key-preserving {@link ParDo}, but we do not have the
   * machinery to detect or enforce that yet.
   *
   * <p>This wraps the {@link GroupByKey} output in a {@link KeyedWorkItem} to be able to deliver
   * timers. It also explodes them into single {@link KV KVs} since this is what the user's {@link
   * DoFn} needs to process anyhow.
   */
  static class ToKeyedWorkItem<K, V>
      extends DoFn<KV<K, Iterable<WindowedValue<KV<K, V>>>>, KeyedWorkItem<K, KV<K, V>>> {

    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      final K key = c.element().getKey();
      c.output(KeyedWorkItems.elementsWorkItem(key, c.element().getValue()));
    }
  }
}
