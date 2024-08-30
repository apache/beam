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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getOnlyElement;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator.TranslationState;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator.UnresolvedTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SideInputValues;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * A {@link TransformTranslator} provides the capability to translate a specific primitive or
 * composite {@link PTransform} into its Spark correspondence.
 *
 * <p>WARNING: {@link TransformTranslator TransformTranslators} should never be serializable! This
 * could easily hide situations where unnecessary references leak into Spark closures.
 */
@Internal
public abstract class TransformTranslator<
    InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>> {

  // Factor to help estimate the complexity of the Spark execution plan. This is used to limit
  // complexity by break linage where necessary to avoid overly large plans. Such plans can become
  // very expensive during planning in the Catalyst optimizer.
  protected final float complexityFactor;

  protected TransformTranslator(float complexityFactor) {
    this.complexityFactor = complexityFactor;
  }

  protected abstract void translate(TransformT transform, Context cxt) throws IOException;

  final void translate(
      TransformT transform,
      AppliedPTransform<InT, OutT, TransformT> appliedTransform,
      TranslationState translationState)
      throws IOException {
    translate(transform, new Context(appliedTransform, translationState));
  }

  /**
   * Checks if a composite / primitive transform can be translated. Composites that cannot be
   * translated as is, will be exploded further for translation of their parts.
   *
   * <p>This returns {@code true} by default and should be overridden where necessary.
   *
   * @throws RuntimeException If a transform uses unsupported features, an exception shall be thrown
   *     to give early feedback before any part of the pipeline is run.
   */
  protected boolean canTranslate(TransformT transform) {
    return true;
  }

  /**
   * Available mutable context to translate a {@link PTransform}. The context is backed by the
   * shared {@link TranslationState} of the {@link PipelineTranslator}.
   */
  protected class Context implements TranslationState {
    private final AppliedPTransform<InT, OutT, TransformT> transform;
    private final TranslationState state;

    private @MonotonicNonNull InT pIn = null;
    private @MonotonicNonNull OutT pOut = null;

    private Context(AppliedPTransform<InT, OutT, TransformT> transform, TranslationState state) {
      this.transform = transform;
      this.state = state;
    }

    public InT getInput() {
      if (pIn == null) {
        pIn = (InT) getOnlyElement(TransformInputs.nonAdditionalInputs(transform));
      }
      return pIn;
    }

    public Map<TupleTag<?>, PCollection<?>> getInputs() {
      return transform.getInputs();
    }

    public Map<TupleTag<?>, PCollection<?>> getOutputs() {
      return transform.getOutputs();
    }

    public OutT getOutput() {
      if (pOut == null) {
        pOut = (OutT) getOnlyElement(transform.getOutputs().values());
      }
      return pOut;
    }

    public <T> PCollection<T> getOutput(TupleTag<T> tag) {
      PCollection<T> pc = (PCollection<T>) transform.getOutputs().get(tag);
      if (pc == null) {
        throw new IllegalStateException("No output for tag " + tag);
      }
      return pc;
    }

    public AppliedPTransform<InT, OutT, TransformT> getCurrentTransform() {
      return transform;
    }

    @Override
    public <T> Dataset<WindowedValue<T>> getDataset(PCollection<T> pCollection) {
      return state.getDataset(pCollection);
    }

    @Override
    public <T> Broadcast<SideInputValues<T>> getSideInputBroadcast(
        PCollection<T> pCollection, SideInputValues.Loader<T> loader) {
      return state.getSideInputBroadcast(pCollection, loader);
    }

    @Override
    public <T> void putDataset(
        PCollection<T> pCollection, Dataset<WindowedValue<T>> dataset, boolean cache) {
      state.putDataset(pCollection, dataset, cache);
    }

    @Override
    public <InputT, T> void putUnresolved(
        PCollection<T> out, UnresolvedTranslation<InputT, T> unresolved) {
      state.putUnresolved(out, unresolved);
    }

    @Override
    public boolean isLeaf(PCollection<?> pCollection) {
      return state.isLeaf(pCollection);
    }

    @Override
    public Supplier<PipelineOptions> getOptionsSupplier() {
      return state.getOptionsSupplier();
    }

    @Override
    public PipelineOptions getOptions() {
      return state.getOptions();
    }

    public <T> Dataset<WindowedValue<T>> createDataset(
        List<WindowedValue<T>> data, Encoder<WindowedValue<T>> enc) {
      return data.isEmpty()
          ? getSparkSession().emptyDataset(enc)
          : getSparkSession().createDataset(data, enc);
    }

    public <T> Broadcast<T> broadcast(T value) {
      return getSparkSession().sparkContext().broadcast(value, (ClassTag) ClassTag.AnyRef());
    }

    @Override
    public SparkSession getSparkSession() {
      return state.getSparkSession();
    }

    @Override
    public <T> Encoder<T> encoderOf(Coder<T> coder, Factory<T> factory) {
      return state.encoderOf(coder, factory);
    }

    public <T1, T2> Encoder<Tuple2<T1, T2>> tupleEncoder(Encoder<T1> e1, Encoder<T2> e2) {
      return Encoders.tuple(e1, e2);
    }

    public <T> Encoder<WindowedValue<T>> windowedEncoder(Coder<T> coder) {
      return windowedValueEncoder(encoderOf(coder), windowEncoder());
    }

    public <T> Encoder<WindowedValue<T>> windowedEncoder(Encoder<T> enc) {
      return windowedValueEncoder(enc, windowEncoder());
    }

    public <T, W extends BoundedWindow> Encoder<WindowedValue<T>> windowedEncoder(
        Coder<T> coder, Coder<W> windowCoder) {
      return windowedValueEncoder(encoderOf(coder), encoderOf(windowCoder));
    }

    public Encoder<BoundedWindow> windowEncoder() {
      checkState(!getInputs().isEmpty(), "Transform has no inputs, cannot get windowCoder!");
      return encoderOf(windowCoder((PCollection) getInput()));
    }
  }

  protected <T> Coder<BoundedWindow> windowCoder(PCollection<T> pc) {
    return (Coder) pc.getWindowingStrategy().getWindowFn().windowCoder();
  }
}
