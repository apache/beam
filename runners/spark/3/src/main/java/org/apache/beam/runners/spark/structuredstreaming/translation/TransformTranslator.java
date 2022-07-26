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

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderFor;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.kvEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
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
 * Supports translation between a Beam transform, and Spark's operations on Datasets.
 *
 * <p>WARNING: Do not make this class serializable! It could easily hide situations where
 * unnecessary references leak into Spark closures.
 */
public abstract class TransformTranslator<
    InT extends PInput, OutT extends POutput, TransformT extends PTransform<? extends InT, OutT>> {

  protected abstract void translate(TransformT transform, Context cxt) throws IOException;

  public final void translate(
      TransformT transform,
      AppliedPTransform<InT, OutT, PTransform<InT, OutT>> appliedTransform,
      TranslationContext cxt)
      throws IOException {
    translate(transform, new Context(appliedTransform, cxt));
  }

  protected class Context {
    private final AppliedPTransform<InT, OutT, PTransform<InT, OutT>> transform;
    private final TranslationContext cxt;
    private @MonotonicNonNull InT pIn = null;
    private @MonotonicNonNull OutT pOut = null;

    protected Context(
        AppliedPTransform<InT, OutT, PTransform<InT, OutT>> transform, TranslationContext cxt) {
      this.transform = transform;
      this.cxt = cxt;
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

    public Map<TupleTag<?>, PCollection<?>> getOutputs() {
      return transform.getOutputs();
    }

    public AppliedPTransform<InT, OutT, PTransform<InT, OutT>> getCurrentTransform() {
      return transform;
    }

    public <T> Dataset<WindowedValue<T>> getDataset(PCollection<T> pCollection) {
      return cxt.getDataset(pCollection);
    }

    public <T> void putDataset(PCollection<T> pCollection, Dataset<WindowedValue<T>> dataset) {
      cxt.putDataset(pCollection, dataset);
    }

    public SerializablePipelineOptions getSerializableOptions() {
      return cxt.getSerializableOptions();
    }

    public PipelineOptions getOptions() {
      return cxt.getSerializableOptions().get();
    }

    // FIXME Types don't guarantee anything!
    public <ViewT, ElemT> void setSideInputDataset(
        PCollectionView<ViewT> value, Dataset<WindowedValue<ElemT>> set) {
      cxt.setSideInputDataset(value, set);
    }

    public <T> Dataset<T> getSideInputDataset(PCollectionView<?> sideInput) {
      return cxt.getSideInputDataSet(sideInput);
    }

    public <T> Dataset<WindowedValue<T>> createDataset(
        List<WindowedValue<T>> data, Encoder<WindowedValue<T>> enc) {
      return data.isEmpty()
          ? cxt.getSparkSession().emptyDataset(enc)
          : cxt.getSparkSession().createDataset(data, enc);
    }

    public <T> Broadcast<T> broadcast(T value) {
      return cxt.getSparkSession().sparkContext().broadcast(value, (ClassTag) ClassTag.AnyRef());
    }

    public SparkSession getSparkSession() {
      return cxt.getSparkSession();
    }

    public <T> Encoder<T> encoderOf(Coder<T> coder) {
      return coder instanceof KvCoder ? kvEncoderOf((KvCoder) coder) : getOrCreateEncoder(coder);
    }

    public <K, V> Encoder<KV<K, V>> kvEncoderOf(KvCoder<K, V> coder) {
      return cxt.encoderOf(coder, c -> kvEncoder(keyEncoderOf(coder), valueEncoderOf(coder)));
    }

    public <K, V> Encoder<K> keyEncoderOf(KvCoder<K, V> coder) {
      return getOrCreateEncoder(coder.getKeyCoder());
    }

    public <K, V> Encoder<V> valueEncoderOf(KvCoder<K, V> coder) {
      return getOrCreateEncoder(coder.getValueCoder());
    }

    public <T> Encoder<WindowedValue<T>> windowedEncoder(Coder<T> coder) {
      return windowedValueEncoder(encoderOf(coder), windowEncoder());
    }

    public <T> Encoder<WindowedValue<T>> windowedEncoder(Encoder<T> enc) {
      return windowedValueEncoder(enc, windowEncoder());
    }

    public <T1, T2> Encoder<Tuple2<T1, T2>> tupleEncoder(Encoder<T1> e1, Encoder<T2> e2) {
      return Encoders.tuple(e1, e2);
    }

    public <T, W extends BoundedWindow> Encoder<WindowedValue<T>> windowedEncoder(
        Coder<T> coder, Coder<W> windowCoder) {
      return windowedValueEncoder(encoderOf(coder), getOrCreateWindowCoder(windowCoder));
    }

    public Encoder<BoundedWindow> windowEncoder() {
      checkState(
          !transform.getInputs().isEmpty(), "Transform has no inputs, cannot get windowCoder!");
      Coder<BoundedWindow> coder =
          ((PCollection) getInput()).getWindowingStrategy().getWindowFn().windowCoder();
      return cxt.encoderOf(coder, c -> encoderFor(c));
    }

    private <W extends BoundedWindow> Encoder<BoundedWindow> getOrCreateWindowCoder(
        Coder<W> coder) {
      return cxt.encoderOf((Coder<BoundedWindow>) coder, c -> encoderFor(c));
    }

    private <T> Encoder<T> getOrCreateEncoder(Coder<T> coder) {
      return cxt.encoderOf(coder, c -> encoderFor(c));
    }
  }

  protected <T> Coder<BoundedWindow> windowCoder(PCollection<T> pc) {
    return (Coder) pc.getWindowingStrategy().getWindowFn().windowCoder();
  }
}
