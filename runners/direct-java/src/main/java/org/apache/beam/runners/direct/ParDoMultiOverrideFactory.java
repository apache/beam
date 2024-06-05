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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A {@link PTransformOverrideFactory} that provides overrides for applications of a {@link ParDo}
 * in the direct runner. Currently overrides applications of <a
 * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a>.
 */
@VisibleForTesting
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ParDoMultiOverrideFactory<InputT, OutputT>
    implements PTransformOverrideFactory<
        PCollection<? extends InputT>,
        PCollectionTuple,
        PTransform<PCollection<? extends InputT>, PCollectionTuple>> {
  @Override
  public PTransformReplacement<PCollection<? extends InputT>, PCollectionTuple>
      getReplacementTransform(
          AppliedPTransform<
                  PCollection<? extends InputT>,
                  PCollectionTuple,
                  PTransform<PCollection<? extends InputT>, PCollectionTuple>>
              application) {

    try {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(application),
          getReplacementForApplication(application));
    } catch (IOException exc) {
      throw new RuntimeException(exc);
    }
  }

  @SuppressWarnings("unchecked")
  private PTransform<PCollection<? extends InputT>, PCollectionTuple> getReplacementForApplication(
      AppliedPTransform<
              PCollection<? extends InputT>,
              PCollectionTuple,
              PTransform<PCollection<? extends InputT>, PCollectionTuple>>
          application)
      throws IOException {

    DoFn<InputT, OutputT> fn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(application);

    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());

    if (signature.processElement().isSplittable()) {
      return SplittableParDo.forAppliedParDo((AppliedPTransform) application);
    } else if (signature.stateDeclarations().size() > 0
        || signature.timerDeclarations().size() > 0
        || signature.timerFamilyDeclarations().size() > 0) {
      return new GbkThenStatefulParDo(
          fn,
          ParDoTranslation.getMainOutputTag(application),
          ParDoTranslation.getAdditionalOutputTags(application),
          ParDoTranslation.getSideInputs(application),
          ParDoTranslation.getSchemaInformation(application),
          ParDoTranslation.getSideInputMapping(application));
    } else {
      return application.getTransform();
    }
  }

  @Override
  public Map<PCollection<?>, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs, PCollectionTuple newOutput) {
    return ReplacementOutputs.tagged(outputs, newOutput);
  }

  static class GbkThenStatefulParDo<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollectionTuple> {
    @SuppressFBWarnings(
        "SE_TRANSIENT_FIELD_NOT_RESTORED") // PTransforms do not actually support serialization.
    private final transient DoFn<KV<K, InputT>, OutputT> doFn;

    private final TupleTagList additionalOutputTags;
    private final TupleTag<OutputT> mainOutputTag;
    private final List<PCollectionView<?>> sideInputs;
    private final DoFnSchemaInformation doFnSchemaInformation;
    private final Map<String, PCollectionView<?>> sideInputMapping;

    public GbkThenStatefulParDo(
        DoFn<KV<K, InputT>, OutputT> doFn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        List<PCollectionView<?>> sideInputs,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<String, PCollectionView<?>> sideInputMapping) {
      this.doFn = doFn;
      this.additionalOutputTags = additionalOutputTags;
      this.mainOutputTag = mainOutputTag;
      this.sideInputs = sideInputs;
      this.doFnSchemaInformation = doFnSchemaInformation;
      this.sideInputMapping = sideInputMapping;
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<K, InputT>> input) {

      PCollection<KeyedWorkItem<K, KV<K, InputT>>> adjustedInput = groupToKeyedWorkItem(input);

      return applyStatefulParDo(adjustedInput);
    }

    @VisibleForTesting
    PCollection<KeyedWorkItem<K, KV<K, InputT>>> groupToKeyedWorkItem(
        PCollection<KV<K, InputT>> input) {

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

      return input
          // Stash the original timestamps, etc, for when it is fed to the user's DoFn
          .apply("Reify timestamps", ParDo.of(new ReifyWindowedValueFn<>()))
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
          .apply("Group by key", GroupByKey.create())

          // Adapt to KeyedWorkItem; that is how this runner delivers timers
          .apply("To KeyedWorkItem", ParDo.of(new ToKeyedWorkItem<>()))
          .setCoder(KeyedWorkItemCoder.of(keyCoder, kvCoder, windowCoder))

          // Because of the intervening GBK, we may have abused the windowing strategy
          // of the input, which should be transferred to the output in a straightforward manner
          // according to what ParDo already does.
          .setWindowingStrategyInternal(inputWindowingStrategy);
    }

    @VisibleForTesting
    PCollectionTuple applyStatefulParDo(
        PCollection<KeyedWorkItem<K, KV<K, InputT>>> adjustedInput) {
      return adjustedInput
          // Explode the resulting iterable into elements that are exactly the ones from
          // the input
          .apply(
          "Stateful ParDo",
          new StatefulParDo<>(
              doFn,
              mainOutputTag,
              additionalOutputTags,
              sideInputs,
              doFnSchemaInformation,
              sideInputMapping));
    }
  }

  static final String DIRECT_STATEFUL_PAR_DO_URN = "beam:directrunner:transforms:stateful_pardo:v1";

  static class StatefulParDo<K, InputT, OutputT>
      extends PTransform<PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple> {
    private final transient DoFn<KV<K, InputT>, OutputT> doFn;
    private final TupleTagList additionalOutputTags;
    private final TupleTag<OutputT> mainOutputTag;
    private final List<PCollectionView<?>> sideInputs;
    private final DoFnSchemaInformation doFnSchemaInformation;
    private final Map<String, PCollectionView<?>> sideInputMapping;

    public StatefulParDo(
        DoFn<KV<K, InputT>, OutputT> doFn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        List<PCollectionView<?>> sideInputs,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<String, PCollectionView<?>> sideInputMapping) {
      this.doFn = doFn;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
      this.sideInputs = sideInputs;
      this.doFnSchemaInformation = doFnSchemaInformation;
      this.sideInputMapping = sideInputMapping;
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

    public DoFnSchemaInformation getSchemaInformation() {
      return doFnSchemaInformation;
    }

    public Map<String, PCollectionView<?>> getSideInputMapping() {
      return sideInputMapping;
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    @Override
    public PCollectionTuple expand(PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>> input) {

      return PCollectionTuple.ofPrimitiveOutputsInternal(
          input.getPipeline(),
          TupleTagList.of(getMainOutputTag()).and(getAdditionalOutputTags().getAll()),
          // TODO
          Collections.emptyMap(),
          input.getWindowingStrategy(),
          input.isBounded());
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
