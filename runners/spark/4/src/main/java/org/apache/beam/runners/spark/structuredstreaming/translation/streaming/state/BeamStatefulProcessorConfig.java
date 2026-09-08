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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Everything {@link BeamStatefulProcessor} needs in order to run a Beam transform inside Spark 4's
 * {@code transformWithState}, shipped to the executors by Java serialisation.
 *
 * <p>Two modes are supported, see {@link Mode}. Both share the same wire format, the same state and
 * timer bridges and the same operator, they differ only in which {@code DoFnRunner} stack is built
 * on the executor.
 *
 * <p>Everything referenced from here must be {@link Serializable}. Beam coders, {@code DoFn}s,
 * {@code WindowingStrategy} and {@code TupleTag} always are; the {@link #sideInputReader()} and the
 * {@link #optionsSupplier()} are the two places where a caller could accidentally pass something
 * that is not, so {@code SparkSideInputReader} and the broadcast options supplier of the evaluation
 * context should be used.
 */
@AutoValue
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BeamStatefulProcessorConfig implements Serializable {

  /** Which Beam execution stack the operator hosts. */
  public enum Mode {
    /**
     * Runs the user's {@code DoFn} through {@code DoFnRunners.simpleRunner} wrapped in {@code
     * DoFnRunners.defaultStatefulDoFnRunner}. Input elements are {@code KV<K, V>}.
     */
    STATEFUL_PARDO,

    /**
     * Runs {@code GroupAlsoByWindowViaWindowSetNewDoFn} with {@code SystemReduceFn.buffering}, fed
     * with {@code KeyedWorkItem}s assembled from the batch's elements and the fired timers. This is
     * how windowed {@code GroupByKey} is implemented. {@link #doFn()} must be unset.
     */
    GROUP_ALSO_BY_WINDOW
  }

  /** A {@link Supplier} of {@link PipelineOptions} that survives Java serialisation. */
  public interface OptionsSupplier extends Supplier<PipelineOptions>, Serializable {}

  /** The execution stack to host. */
  public abstract Mode mode();

  /**
   * The user's {@code DoFn} for {@link Mode#STATEFUL_PARDO}, {@code null} for {@link
   * Mode#GROUP_ALSO_BY_WINDOW} where the operator builds the group-also-by-window {@code DoFn}
   * itself.
   */
  public abstract @Nullable DoFn<?, ?> doFn();

  /** Coder of the Beam key {@code K}, used to decode the {@code byte[]} grouping key. */
  public abstract Coder<?> keyCoder();

  /**
   * Coder of the element value {@code V}, that is the value side of the input {@code KV<K, V>}.
   *
   * <p>It is also the element coder of the {@code SystemReduceFn.buffering} buffer in {@link
   * Mode#GROUP_ALSO_BY_WINDOW}.
   */
  public abstract Coder<?> valueCoder();

  /** Windowing strategy of the input {@code PCollection}. */
  public abstract WindowingStrategy<?, ?> windowingStrategy();

  /** The main output tag, always output index {@code 0}. */
  public abstract TupleTag<?> mainOutputTag();

  /** Additional output tags, output indexes {@code 1..n} in this order. */
  public abstract List<TupleTag<?>> additionalOutputTags();

  /** Element coder per output tag; must contain an entry for every tag in {@link #outputTags()}. */
  public abstract Map<TupleTag<?>, Coder<?>> outputCoders();

  /** Reader for statically broadcast side inputs, must be serializable. */
  public abstract SideInputReader sideInputReader();

  /** Side input mapping passed to {@code DoFnRunners.simpleRunner}. */
  public abstract Map<String, PCollectionView<?>> sideInputMapping();

  /** Schema information of the hosted {@code DoFn}. */
  public abstract DoFnSchemaInformation doFnSchemaInformation();

  /** Supplies the pipeline options on the executor. */
  public abstract OptionsSupplier optionsSupplier();

  /** Human readable step name, used in error messages only. */
  public abstract String stepName();

  /** The window coder of {@link #windowingStrategy()}. */
  public final Coder<? extends BoundedWindow> windowCoder() {
    return windowingStrategy().getWindowFn().windowCoder();
  }

  /** Full windowed value coder of the input element value, the input row payload coder. */
  public final <V> WindowedValues.FullWindowedValueCoder<V> inputValueCoder() {
    return WindowedValues.getFullCoder((Coder<V>) valueCoder(), windowCoder());
  }

  /**
   * The element coder of the input as the hosted {@code DoFn} sees it in {@link
   * Mode#STATEFUL_PARDO}, that is {@code KvCoder.of(keyCoder(), valueCoder())}.
   */
  public final <K, V> KvCoder<K, V> kvInputCoder() {
    return KvCoder.of((Coder<K>) keyCoder(), (Coder<V>) valueCoder());
  }

  /** All output tags, main first, in output index order. */
  public final List<TupleTag<?>> outputTags() {
    List<TupleTag<?>> tags = new ArrayList<>(additionalOutputTags().size() + 1);
    tags.add(mainOutputTag());
    tags.addAll(additionalOutputTags());
    return tags;
  }

  /** Full windowed value coder of the elements emitted on {@code tag}. */
  public final <T> WindowedValues.FullWindowedValueCoder<T> outputCoderFor(TupleTag<?> tag) {
    Coder<?> coder = outputCoders().get(tag);
    if (coder == null) {
      throw new IllegalArgumentException("No output coder configured for tag " + tag);
    }
    return WindowedValues.getFullCoder((Coder<T>) coder, windowCoder());
  }

  /** Returns a builder with the optional properties already defaulted. */
  public static Builder builder() {
    return new AutoValue_BeamStatefulProcessorConfig.Builder()
        .setAdditionalOutputTags(Collections.emptyList())
        .setOutputCoders(Collections.emptyMap())
        .setSideInputReader(EmptySideInputReader.INSTANCE)
        .setSideInputMapping(Collections.emptyMap())
        .setDoFnSchemaInformation(DoFnSchemaInformation.create())
        .setStepName("");
  }

  /** Builder for {@link BeamStatefulProcessorConfig}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setMode(Mode mode);

    public abstract Builder setDoFn(@Nullable DoFn<?, ?> doFn);

    public abstract Builder setKeyCoder(Coder<?> keyCoder);

    public abstract Builder setValueCoder(Coder<?> valueCoder);

    public abstract Builder setWindowingStrategy(WindowingStrategy<?, ?> windowingStrategy);

    public abstract Builder setMainOutputTag(TupleTag<?> mainOutputTag);

    public abstract Builder setAdditionalOutputTags(List<TupleTag<?>> additionalOutputTags);

    public abstract Builder setOutputCoders(Map<TupleTag<?>, Coder<?>> outputCoders);

    public abstract Builder setSideInputReader(SideInputReader sideInputReader);

    public abstract Builder setSideInputMapping(Map<String, PCollectionView<?>> sideInputMapping);

    public abstract Builder setDoFnSchemaInformation(DoFnSchemaInformation doFnSchemaInformation);

    public abstract Builder setOptionsSupplier(OptionsSupplier optionsSupplier);

    public abstract Builder setStepName(String stepName);

    abstract BeamStatefulProcessorConfig autoBuild();

    public BeamStatefulProcessorConfig build() {
      BeamStatefulProcessorConfig config = autoBuild();
      if (config.mode() == Mode.STATEFUL_PARDO) {
        checkArgument(config.doFn() != null, "STATEFUL_PARDO requires a DoFn");
      } else {
        checkArgument(
            config.doFn() == null,
            "GROUP_ALSO_BY_WINDOW builds its own DoFn, no DoFn may be configured");
      }
      for (TupleTag<?> tag : config.outputTags()) {
        checkArgument(
            config.outputCoders().containsKey(tag), "No output coder configured for tag %s", tag);
      }
      return config;
    }
  }

  /** Serializable no side input reader, the default of {@link #sideInputReader()}. */
  private static class EmptySideInputReader implements SideInputReader, Serializable {
    private static final EmptySideInputReader INSTANCE = new EmptySideInputReader();

    @Override
    public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
      throw new IllegalArgumentException(
          "No side inputs were configured on this stateful operator, cannot read " + view);
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return false;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }
}
