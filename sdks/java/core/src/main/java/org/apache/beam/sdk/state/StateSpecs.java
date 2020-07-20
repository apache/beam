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
package org.apache.beam.sdk.state;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Static methods for working with {@link StateSpec StateSpecs}. */
@Experimental(Kind.STATE)
public class StateSpecs {

  private static final CoderRegistry STANDARD_REGISTRY = CoderRegistry.createDefault();

  private StateSpecs() {}

  /**
   * Create a {@link StateSpec} for a single value of type {@code T}.
   *
   * <p>This method attempts to infer the value coder automatically.
   *
   * <p>If the value type has a schema registered, then the schema will be used to encode the
   * values.
   *
   * @see #value(Coder)
   */
  public static <T> StateSpec<ValueState<T>> value() {
    return new ValueStateSpec<>(null);
  }

  /** Create a {@link StateSpec} for a row value with the specified schema. */
  public static StateSpec<ValueState<Row>> rowValue(Schema schema) {
    return value(RowCoder.of(schema));
  }

  /**
   * Identical to {@link #value()}, but with a coder explicitly supplied.
   *
   * <p>If automatic coder inference fails, use this method.
   */
  public static <T> StateSpec<ValueState<T>> value(Coder<T> valueCoder) {
    checkArgument(valueCoder != null, "valueCoder should not be null. Consider value() instead");
    return new ValueStateSpec<>(valueCoder);
  }

  /**
   * Create a {@link StateSpec} for a {@link CombiningState} which uses a {@link CombineFn} to
   * automatically merge multiple values of type {@code InputT} into a single resulting {@code
   * OutputT}.
   *
   * <p>This method attempts to infer the accumulator coder automatically.
   *
   * @see #combining(Coder, CombineFn)
   */
  public static <InputT, AccumT, OutputT>
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combining(
          CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new CombiningStateSpec<>(null, combineFn);
  }

  /**
   * <b>For internal use only; no backwards compatibility guarantees</b>
   *
   * <p>Create a {@link StateSpec} for a {@link CombiningState} which uses a {@link
   * CombineFnWithContext} to automatically merge multiple values of type {@code InputT} into a
   * single resulting {@code OutputT}.
   *
   * <p>This method attempts to infer the accumulator coder automatically.
   *
   * @see #combining(Coder, CombineFnWithContext)
   */
  @Internal
  public static <InputT, AccumT, OutputT>
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combining(
          CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
    return new CombiningWithContextStateSpec<>(null, combineFn);
  }

  /**
   * Identical to {@link #combining(CombineFn)}, but with an accumulator coder explicitly supplied.
   *
   * <p>If automatic coder inference fails, use this method.
   */
  public static <InputT, AccumT, OutputT>
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combining(
          Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    checkArgument(
        accumCoder != null,
        "accumCoder should not be null. "
            + "Consider using combining(CombineFn<> combineFn) instead.");
    return combiningInternal(accumCoder, combineFn);
  }

  /**
   * <b>For internal use only; no backwards compatibility guarantees</b>
   *
   * <p>Identical to {@link #combining(CombineFnWithContext)}, but with an accumulator coder
   * explicitly supplied.
   *
   * <p>If automatic coder inference fails, use this method.
   */
  @Internal
  public static <InputT, AccumT, OutputT>
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combining(
          Coder<AccumT> accumCoder, CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
    return combiningInternal(accumCoder, combineFn);
  }

  /**
   * Create a {@link StateSpec} for a {@link BagState}, optimized for adding values frequently and
   * occasionally retrieving all the values that have been added.
   *
   * <p>This method attempts to infer the element coder automatically.
   *
   * <p>If the element type has a schema registered, then the schema will be used to encode the
   * values.
   *
   * @see #bag(Coder)
   */
  public static <T> StateSpec<BagState<T>> bag() {
    return new BagStateSpec<>(null);
  }

  /**
   * Create a {@link StateSpec} for a {@link BagState}, optimized for adding values frequently and
   * occasionally retrieving all the values that have been added.
   *
   * <p>This method is for storing row elements with the given schema.
   */
  public static StateSpec<BagState<Row>> rowBag(Schema schema) {
    return new BagStateSpec<>(RowCoder.of(schema));
  }

  /**
   * Identical to {@link #bag()}, but with an element coder explicitly supplied.
   *
   * <p>If automatic coder inference fails, use this method.
   */
  public static <T> StateSpec<BagState<T>> bag(Coder<T> elemCoder) {
    return new BagStateSpec<>(elemCoder);
  }

  /**
   * Create a {@link StateSpec} for a {@link SetState}, optimized for checking membership.
   *
   * <p>This method attempts to infer the element coder automatically.
   *
   * <p>If the element type has a schema registered, then the schema will be used to encode the
   * values.
   *
   * @see #set(Coder)
   */
  public static <T> StateSpec<SetState<T>> set() {
    return new SetStateSpec<>(null);
  }

  /**
   * Create a {@link StateSpec} for a {@link SetState}, optimized for checking membership.
   *
   * <p>This method is for storing row elements with the given schema.
   */
  public static StateSpec<SetState<Row>> rowSet(Schema schema) {
    return new SetStateSpec<>(RowCoder.of(schema));
  }

  /**
   * Identical to {@link #set()}, but with an element coder explicitly supplied.
   *
   * <p>If automatic coder inference fails, use this method.
   */
  public static <T> StateSpec<SetState<T>> set(Coder<T> elemCoder) {
    return new SetStateSpec<>(elemCoder);
  }

  /**
   * Create a {@link StateSpec} for a {@link SetState}, optimized for key lookups and writes.
   *
   * <p>This method attempts to infer the key and value coders automatically.
   *
   * <p>If the key and value types have schemas registered, then the schemas will be used to encode
   * the elements.
   *
   * @see #map(Coder, Coder)
   */
  public static <K, V> StateSpec<MapState<K, V>> map() {
    return new MapStateSpec<>(null, null);
  }

  /**
   * Create a {@link StateSpec} for a {@link SetState}, optimized for key lookups and writes.
   *
   * <p>This method is for storing maps where both the keys and the values are rows with the
   * specified schemas.
   *
   * @see #map(Coder, Coder)
   */
  public static StateSpec<MapState<Row, Row>> rowMap(Schema keySchema, Schema valueSchema) {
    return new MapStateSpec<>(RowCoder.of(keySchema), RowCoder.of(valueSchema));
  }

  /**
   * Identical to {@link #map()}, but with key and value coders explicitly supplied.
   *
   * <p>If automatic coder inference fails, use this method.
   */
  public static <K, V> StateSpec<MapState<K, V>> map(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new MapStateSpec<>(keyCoder, valueCoder);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Create a state spec for values that use a {@link CombineFn} to automatically merge multiple
   * {@code InputT}s into a single {@code OutputT}.
   *
   * <p>This determines the {@code Coder<AccumT>} from the given {@code Coder<InputT>}, and should
   * only be used to initialize static values.
   */
  @Internal
  public static <InputT, AccumT, OutputT>
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combiningFromInputInternal(
          Coder<InputT> inputCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    try {
      Coder<AccumT> accumCoder = combineFn.getAccumulatorCoder(STANDARD_REGISTRY, inputCoder);
      return combiningInternal(accumCoder, combineFn);
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
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combiningInternal(
          Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new CombiningStateSpec<>(accumCoder, combineFn);
  }

  private static <InputT, AccumT, OutputT>
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combiningInternal(
          Coder<AccumT> accumCoder, CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
    return new CombiningWithContextStateSpec<>(accumCoder, combineFn);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Create a state spec for a watermark hold.
   */
  @Internal
  public static StateSpec<WatermarkHoldState> watermarkStateInternal(
      TimestampCombiner timestampCombiner) {
    return new WatermarkStateSpecInternal(timestampCombiner);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Convert a combining state spec to a bag of accumulators.
   */
  @Internal
  public static <InputT, AccumT, OutputT> StateSpec<BagState<AccumT>> convertToBagSpecInternal(
      StateSpec<CombiningState<InputT, AccumT, OutputT>> combiningSpec) {
    if (combiningSpec instanceof CombiningStateSpec) {
      // Checked above; conversion to a bag spec depends on the provided spec being one of those
      // created via the factory methods in this class.
      @SuppressWarnings("unchecked")
      CombiningStateSpec<InputT, AccumT, OutputT> typedSpec =
          (CombiningStateSpec<InputT, AccumT, OutputT>) combiningSpec;
      return typedSpec.asBagSpec();
    } else if (combiningSpec instanceof CombiningWithContextStateSpec) {
      @SuppressWarnings("unchecked")
      CombiningWithContextStateSpec<InputT, AccumT, OutputT> typedSpec =
          (CombiningWithContextStateSpec<InputT, AccumT, OutputT>) combiningSpec;
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
  private static class ValueStateSpec<T> implements StateSpec<ValueState<T>> {

    private @Nullable Coder<T> coder;

    private ValueStateSpec(@Nullable Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public ValueState<T> bind(String id, StateBinder visitor) {
      return visitor.bindValue(id, this, coder);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      return cases.dispatchValue(coder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void offerCoders(Coder[] coders) {
      if (this.coder == null && coders[0] != null) {
        this.coder = (Coder<T>) coders[0];
      }
    }

    @Override
    public void finishSpecifying() {
      if (coder == null) {
        throw new IllegalStateException(
            "Unable to infer a coder for ValueState and no Coder"
                + " was specified. Please set a coder by either invoking"
                + " StateSpecs.value(Coder<T> valueCoder) or by registering the coder in the"
                + " Pipeline's CoderRegistry.");
      }
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
  private static class CombiningStateSpec<InputT, AccumT, OutputT>
      implements StateSpec<CombiningState<InputT, AccumT, OutputT>> {

    private @Nullable Coder<AccumT> accumCoder;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private CombiningStateSpec(
        @Nullable Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> bind(String id, StateBinder visitor) {
      return visitor.bindCombining(id, this, accumCoder, combineFn);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      return cases.dispatchCombining(combineFn, accumCoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void offerCoders(Coder[] coders) {
      if (this.accumCoder == null && coders[1] != null) {
        this.accumCoder = (Coder<AccumT>) coders[1];
      }
    }

    @Override
    public void finishSpecifying() {
      if (accumCoder == null) {
        throw new IllegalStateException(
            "Unable to infer a coder for"
                + " CombiningState and no Coder was specified."
                + " Please set a coder by either invoking"
                + " StateSpecs.combining(Coder<AccumT> accumCoder,"
                + " CombineFn<InputT, AccumT, OutputT> combineFn)"
                + " or by registering the coder in the Pipeline's CoderRegistry.");
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof CombiningStateSpec)) {
        return false;
      }

      CombiningStateSpec<?, ?, ?> that = (CombiningStateSpec<?, ?, ?>) obj;
      return Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), accumCoder);
    }

    private StateSpec<BagState<AccumT>> asBagSpec() {
      return new BagStateSpec<>(accumCoder);
    }
  }

  /**
   * A specification for a state cell that is combined according to a {@link CombineFnWithContext}.
   *
   * <p>Includes the {@link CombineFnWithContext} and the coder for the accumulator type.
   */
  private static class CombiningWithContextStateSpec<InputT, AccumT, OutputT>
      implements StateSpec<CombiningState<InputT, AccumT, OutputT>> {

    private @Nullable Coder<AccumT> accumCoder;
    private final CombineFnWithContext<InputT, AccumT, OutputT> combineFn;

    private CombiningWithContextStateSpec(
        @Nullable Coder<AccumT> accumCoder,
        CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public CombiningState<InputT, AccumT, OutputT> bind(String id, StateBinder visitor) {
      return visitor.bindCombiningWithContext(id, this, accumCoder, combineFn);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is for internal use only and does not support case dispatch",
              getClass().getSimpleName()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void offerCoders(Coder[] coders) {
      if (this.accumCoder == null && coders[2] != null) {
        this.accumCoder = (Coder<AccumT>) coders[2];
      }
    }

    @Override
    public void finishSpecifying() {
      if (accumCoder == null) {
        throw new IllegalStateException(
            "Unable to infer a coder for"
                + " CombiningWithContextState and no Coder was specified."
                + " Please set a coder by either invoking"
                + " StateSpecs.combiningWithcontext(Coder<AccumT> accumCoder,"
                + " CombineFnWithContext<InputT, AccumT, OutputT> combineFn)"
                + " or by registering the coder in the Pipeline's CoderRegistry.");
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof CombiningWithContextStateSpec)) {
        return false;
      }

      CombiningWithContextStateSpec<?, ?, ?> that = (CombiningWithContextStateSpec<?, ?, ?>) obj;
      return Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), accumCoder);
    }

    private StateSpec<BagState<AccumT>> asBagSpec() {
      return new BagStateSpec<>(accumCoder);
    }
  }

  /**
   * A specification for a state cell supporting for bag-like access patterns (frequent additions,
   * occasional reads of all the values).
   *
   * <p>Includes the coder for the element type {@code T}
   */
  private static class BagStateSpec<T> implements StateSpec<BagState<T>> {

    private @Nullable Coder<T> elemCoder;

    private BagStateSpec(@Nullable Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public BagState<T> bind(String id, StateBinder visitor) {
      return visitor.bindBag(id, this, elemCoder);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      return cases.dispatchBag(elemCoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void offerCoders(Coder[] coders) {
      if (this.elemCoder == null && coders[0] != null) {
        this.elemCoder = (Coder<T>) coders[0];
      }
    }

    @Override
    public void finishSpecifying() {
      if (elemCoder == null) {
        throw new IllegalStateException(
            "Unable to infer a coder for BagState and no Coder"
                + " was specified. Please set a coder by either invoking"
                + " StateSpecs.bag(Coder<T> elemCoder) or by registering the coder in the"
                + " Pipeline's CoderRegistry.");
      }
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

  private static class MapStateSpec<K, V> implements StateSpec<MapState<K, V>> {

    private @Nullable Coder<K> keyCoder;
    private @Nullable Coder<V> valueCoder;

    private MapStateSpec(@Nullable Coder<K> keyCoder, @Nullable Coder<V> valueCoder) {
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
    }

    @Override
    public MapState<K, V> bind(String id, StateBinder visitor) {
      return visitor.bindMap(id, this, keyCoder, valueCoder);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      return cases.dispatchMap(keyCoder, valueCoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void offerCoders(Coder[] coders) {
      if (this.keyCoder == null && coders[0] != null) {
        this.keyCoder = (Coder<K>) coders[0];
      }
      if (this.valueCoder == null && coders[1] != null) {
        this.valueCoder = (Coder<V>) coders[1];
      }
    }

    @Override
    public void finishSpecifying() {
      if (keyCoder == null || valueCoder == null) {
        throw new IllegalStateException(
            "Unable to infer a coder for MapState and no Coder"
                + " was specified. Please set a coder by either invoking"
                + " StateSpecs.map(Coder<K> keyCoder, Coder<V> valueCoder) or by registering the"
                + " coder in the Pipeline's CoderRegistry.");
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof MapStateSpec)) {
        return false;
      }

      MapStateSpec<?, ?> that = (MapStateSpec<?, ?>) obj;
      return Objects.equals(this.keyCoder, that.keyCoder)
          && Objects.equals(this.valueCoder, that.valueCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), keyCoder, valueCoder);
    }
  }

  /**
   * A specification for a state cell supporting for set-like access patterns.
   *
   * <p>Includes the coder for the element type {@code T}
   */
  private static class SetStateSpec<T> implements StateSpec<SetState<T>> {

    private @Nullable Coder<T> elemCoder;

    private SetStateSpec(@Nullable Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }

    @Override
    public SetState<T> bind(String id, StateBinder visitor) {
      return visitor.bindSet(id, this, elemCoder);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      return cases.dispatchSet(elemCoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void offerCoders(Coder[] coders) {
      if (this.elemCoder == null && coders[0] != null) {
        this.elemCoder = (Coder<T>) coders[0];
      }
    }

    @Override
    public void finishSpecifying() {
      if (elemCoder == null) {
        throw new IllegalStateException(
            "Unable to infer a coder for SetState and no Coder"
                + " was specified. Please set a coder by either invoking"
                + " StateSpecs.set(Coder<T> elemCoder) or by registering the coder in the"
                + " Pipeline's CoderRegistry.");
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof SetStateSpec)) {
        return false;
      }

      SetStateSpec<?> that = (SetStateSpec<?>) obj;
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
   * <p>Includes the {@link TimestampCombiner} according to which the output times are combined.
   */
  private static class WatermarkStateSpecInternal implements StateSpec<WatermarkHoldState> {

    /**
     * When multiple output times are added to hold the watermark, this determines how they are
     * combined, and also the behavior when merging windows. Does not contribute to equality/hash
     * since we have at most one watermark hold spec per computation.
     */
    private final TimestampCombiner timestampCombiner;

    private WatermarkStateSpecInternal(TimestampCombiner timestampCombiner) {
      this.timestampCombiner = timestampCombiner;
    }

    @Override
    public WatermarkHoldState bind(String id, StateBinder visitor) {
      return visitor.bindWatermark(id, this, timestampCombiner);
    }

    @Override
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is for internal use only and does not support case dispatch",
              getClass().getSimpleName()));
    }

    @Override
    public void offerCoders(Coder[] coders) {}

    @Override
    public void finishSpecifying() {
      // Currently an empty implementation as there are no coders to validate.
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
}
