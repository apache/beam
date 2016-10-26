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
package org.apache.beam.sdk.util.state;

import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;

/**
 * Static utility methods for creating {@link StateSpec} instances.
 */
@Experimental(Kind.STATE)
public class StateSpecs {

  private static final CoderRegistry STANDARD_REGISTRY = new CoderRegistry();

  static {
    STANDARD_REGISTRY.registerStandardCoders();
  }

  private StateSpecs() {}

  /** Create a simple state spec for values of type {@code T}. */
  public static <T> StateSpec<Object, ValueState<T>> value(Coder<T> valueCoder) {
    return new ValueStateSpec<>(valueCoder);
  }

  /**
   * Create a state spec for values that use a {@link CombineFn} to automatically merge multiple
   * {@code InputT}s into a single {@code OutputT}.
   */
  public static <InputT, AccumT, OutputT>
      StateSpec<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> combiningValue(
          Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return combiningValueInternal(accumCoder, combineFn);
  }

  /**
   * Create a state spec for values that use a {@link KeyedCombineFn} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}. The key provided to the {@link
   * KeyedCombineFn} comes from the keyed {@link StateAccessor}.
   */
  public static <K, InputT, AccumT, OutputT>
      StateSpec<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> keyedCombiningValue(
          Coder<AccumT> accumCoder, KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    return keyedCombiningValueInternal(accumCoder, combineFn);
  }

  /**
   * Create a state spec for values that use a {@link KeyedCombineFnWithContext} to automatically
   * merge multiple {@code InputT}s into a single {@code OutputT}. The key provided to the {@link
   * KeyedCombineFn} comes from the keyed {@link StateAccessor}, the context provided comes from the
   * {@link StateContext}.
   */
  public static <K, InputT, AccumT, OutputT>
      StateSpec<K, AccumulatorCombiningState<InputT, AccumT, OutputT>>
          keyedCombiningValueWithContext(
              Coder<AccumT> accumCoder,
              KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn) {
    return new KeyedCombiningValueWithContextStateSpec<K, InputT, AccumT, OutputT>(
        accumCoder, combineFn);
  }

  /**
   * Create a state spec for values that use a {@link CombineFn} to automatically merge multiple
   * {@code InputT}s into a single {@code OutputT}.
   *
   * <p>This determines the {@code Coder<AccumT>} from the given {@code Coder<InputT>}, and should
   * only be used to initialize static values.
   */
  public static <InputT, AccumT, OutputT>
      StateSpec<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>
          combiningValueFromInputInternal(
              Coder<InputT> inputCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    try {
      Coder<AccumT> accumCoder = combineFn.getAccumulatorCoder(STANDARD_REGISTRY, inputCoder);
      return combiningValueInternal(accumCoder, combineFn);
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException(
          "Unable to determine accumulator coder for "
              + combineFn.getClass().getSimpleName()
              + " from "
              + inputCoder,
          e);
    }
  }

  private static <InputT, AccumT, OutputT>
      StateSpec<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> combiningValueInternal(
          Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new CombiningValueStateSpec<InputT, AccumT, OutputT>(accumCoder, combineFn);
  }

  private static <K, InputT, AccumT, OutputT>
      StateSpec<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> keyedCombiningValueInternal(
          Coder<AccumT> accumCoder, KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    return new KeyedCombiningValueStateSpec<K, InputT, AccumT, OutputT>(accumCoder, combineFn);
  }

  /**
   * Create a state spec that is optimized for adding values frequently, and occasionally retrieving
   * all the values that have been added.
   */
  public static <T> StateSpec<Object, BagState<T>> bag(Coder<T> elemCoder) {
    return new BagStateSpec<T>(elemCoder);
  }

  /** Create a state spec for holding the watermark. */
  public static <W extends BoundedWindow>
      StateSpec<Object, WatermarkHoldState<W>> watermarkStateInternal(
          OutputTimeFn<? super W> outputTimeFn) {
    return new WatermarkStateSpecInternal<W>(outputTimeFn);
  }

  public static <K, InputT, AccumT, OutputT>
      StateSpec<Object, BagState<AccumT>> convertToBagSpecInternal(
          StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> combiningSpec) {
    if (combiningSpec instanceof KeyedCombiningValueStateSpec) {
      // Checked above; conversion to a bag spec depends on the provided spec being one of those
      // created via the factory methods in this class.
      @SuppressWarnings("unchecked")
      KeyedCombiningValueStateSpec<K, InputT, AccumT, OutputT> typedSpec =
          (KeyedCombiningValueStateSpec<K, InputT, AccumT, OutputT>) combiningSpec;
      return typedSpec.asBagSpec();
    } else if (combiningSpec instanceof KeyedCombiningValueWithContextStateSpec) {
      @SuppressWarnings("unchecked")
      KeyedCombiningValueWithContextStateSpec<K, InputT, AccumT, OutputT> typedSpec =
          (KeyedCombiningValueWithContextStateSpec<K, InputT, AccumT, OutputT>) combiningSpec;
      return typedSpec.asBagSpec();
    } else {
      throw new IllegalArgumentException("Unexpected StateSpec " + combiningSpec);
    }
  }

  /**
   * A specification for a state cell holding a settable value of type {@code T}.
   *
   * <p>Includes the coder for {@code T}.
   */
  private static class ValueStateSpec<T> implements StateSpec<Object, ValueState<T>> {

    private final Coder<T> coder;

    private ValueStateSpec(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public ValueState<T> bind(String id, StateBinder<?> visitor) {
      return visitor.bindValue(id, this, coder);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof ValueStateSpec)) {
        return false;
      }

      ValueStateSpec<?> that = (ValueStateSpec<?>) obj;
      return Objects.equals(this.coder, that.coder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), coder);
    }
  }

  /**
   * A specification for a state cell that is combined according to a {@link CombineFn}.
   *
   * <p>Includes the {@link CombineFn} and the coder for the accumulator type.
   */
  private static class CombiningValueStateSpec<InputT, AccumT, OutputT>
      extends KeyedCombiningValueStateSpec<Object, InputT, AccumT, OutputT>
      implements StateSpec<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> {

    private final Coder<AccumT> accumCoder;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private CombiningValueStateSpec(
        Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(accumCoder, combineFn.asKeyedFn());
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }
  }

  /**
   * A specification for a state cell that is combined according to a
   * {@link KeyedCombineFnWithContext}.
   *
   * <p>Includes the {@link KeyedCombineFnWithContext} and the coder for the accumulator type.
   */
  private static class KeyedCombiningValueWithContextStateSpec<K, InputT, AccumT, OutputT>
      implements StateSpec<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> {

    private final Coder<AccumT> accumCoder;
    private final KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn;

    protected KeyedCombiningValueWithContextStateSpec(
        Coder<AccumT> accumCoder, KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> bind(
        String id, StateBinder<? extends K> visitor) {
      return visitor.bindKeyedCombiningValueWithContext(id, this, accumCoder, combineFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof KeyedCombiningValueWithContextStateSpec)) {
        return false;
      }

      KeyedCombiningValueWithContextStateSpec<?, ?, ?, ?> that =
          (KeyedCombiningValueWithContextStateSpec<?, ?, ?, ?>) obj;
      return Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), accumCoder);
    }

    private StateSpec<Object, BagState<AccumT>> asBagSpec() {
      return new BagStateSpec<AccumT>(accumCoder);
    }
  }

  /**
   * A specification for a state cell that is combined according to a {@link KeyedCombineFn}.
   *
   * <p>Includes the {@link KeyedCombineFn} and the coder for the accumulator type.
   */
  private static class KeyedCombiningValueStateSpec<K, InputT, AccumT, OutputT>
      implements StateSpec<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> {

    private final Coder<AccumT> accumCoder;
    private final KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn;

    protected KeyedCombiningValueStateSpec(
        Coder<AccumT> accumCoder, KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn) {
      this.keyedCombineFn = keyedCombineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> bind(
        String id, StateBinder<? extends K> visitor) {
      return visitor.bindKeyedCombiningValue(id, this, accumCoder, keyedCombineFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof CombiningValueStateSpec)) {
        return false;
      }

      KeyedCombiningValueStateSpec<?, ?, ?, ?> that =
          (KeyedCombiningValueStateSpec<?, ?, ?, ?>) obj;
      return Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), accumCoder);
    }

    private StateSpec<Object, BagState<AccumT>> asBagSpec() {
      return new BagStateSpec<AccumT>(accumCoder);
    }
  }

  /**
   * A specification for a state cell supporting for bag-like access patterns
   * (frequent additions, occasional reads of all the values).
   *
   * <p>Includes the coder for the element type {@code T}</p>
   */
  private static class BagStateSpec<T> implements StateSpec<Object, BagState<T>> {

    private final Coder<T> elemCoder;

    private BagStateSpec(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public BagState<T> bind(String id, StateBinder<?> visitor) {
      return visitor.bindBag(id, this, elemCoder);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof BagStateSpec)) {
        return false;
      }

      BagStateSpec<?> that = (BagStateSpec<?>) obj;
      return Objects.equals(this.elemCoder, that.elemCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), elemCoder);
    }
  }

  /**
   * A specification for a state cell tracking a combined watermark hold.
   *
   * <p>Includes the {@link OutputTimeFn} according to which the output times
   * are combined.
   */
  private static class WatermarkStateSpecInternal<W extends BoundedWindow>
      implements StateSpec<Object, WatermarkHoldState<W>> {

    /**
     * When multiple output times are added to hold the watermark, this determines how they are
     * combined, and also the behavior when merging windows. Does not contribute to equality/hash
     * since we have at most one watermark hold spec per computation.
     */
    private final OutputTimeFn<? super W> outputTimeFn;

    private WatermarkStateSpecInternal(OutputTimeFn<? super W> outputTimeFn) {
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public WatermarkHoldState<W> bind(String id, StateBinder<?> visitor) {
      return visitor.bindWatermark(id, this, outputTimeFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      // All instance of WatermarkHoldState are considered equal
      return obj instanceof WatermarkStateSpecInternal;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass());
    }
  }

  /**
   * @deprecated for migration purposes only
   */
  @Deprecated
  public static <K> StateBinder<K> adaptTagBinder(final StateTag.StateBinder<K> binder) {
    return new StateBinder<K>() {
      @Override
      public <T> ValueState<T> bindValue(
          String id, StateSpec<? super K, ValueState<T>> spec, Coder<T> coder) {
        return binder.bindValue(StateTags.tagForSpec(id, spec), coder);
      }

      @Override
      public <T> BagState<T> bindBag(
          String id, StateSpec<? super K, BagState<T>> spec, Coder<T> elemCoder) {
        return binder.bindBag(StateTags.tagForSpec(id, spec), elemCoder);
      }

      @Override
      public <InputT, AccumT, OutputT>
          AccumulatorCombiningState<InputT, AccumT, OutputT> bindCombiningValue(
              String id,
              StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFn<InputT, AccumT, OutputT> combineFn) {
        return binder.bindCombiningValue(StateTags.tagForSpec(id, spec), accumCoder, combineFn);
      }

      @Override
      public <InputT, AccumT, OutputT>
          AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(
              String id,
              StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
        return binder.bindKeyedCombiningValue(
            StateTags.tagForSpec(id, spec), accumCoder, combineFn);
      }

      @Override
      public <InputT, AccumT, OutputT>
          AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(
              String id,
              StateSpec<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
        return binder.bindKeyedCombiningValueWithContext(
            StateTags.tagForSpec(id, spec), accumCoder, combineFn);
      }

      @Override
      public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
          String id,
          StateSpec<? super K, WatermarkHoldState<W>> spec,
          OutputTimeFn<? super W> outputTimeFn) {
        return binder.bindWatermark(StateTags.tagForSpec(id, spec), outputTimeFn);
      }
    };
  }
}
