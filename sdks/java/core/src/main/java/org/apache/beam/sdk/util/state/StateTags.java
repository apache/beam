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

import com.google.common.base.MoreObjects;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Static utility methods for creating {@link StateTag} instances.
 */
@Experimental(Kind.STATE)
public class StateTags {

  private static final CoderRegistry STANDARD_REGISTRY = new CoderRegistry();

  static {
    STANDARD_REGISTRY.registerStandardCoders();
  }

  private enum StateKind {
    SYSTEM('s'),
    USER('u');

    private char prefix;

    StateKind(char prefix) {
      this.prefix = prefix;
    }
  }

  private StateTags() { }

  private interface SystemStateTag<K, StateT extends State> {
    StateTag<K, StateT> asKind(StateKind kind);
  }

  /**
   * Create a simple state tag for values of type {@code T}.
   */
  public static <T> StateTag<Object, ValueState<T>> value(String id, Coder<T> valueCoder) {
    return new ValueStateTag<>(new StructuredId(id), valueCoder);
  }

  /**
   * Create a state tag for values that use a {@link CombineFn} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}.
   */
  public static <InputT, AccumT, OutputT>
    StateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>
    combiningValue(
      String id, Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return combiningValueInternal(id, accumCoder, combineFn);
  }

  /**
   * Create a state tag for values that use a {@link KeyedCombineFn} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}. The key provided to the
   * {@link KeyedCombineFn} comes from the keyed {@link StateAccessor}.
   */
  public static <K, InputT, AccumT,
      OutputT> StateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>>
      keyedCombiningValue(String id, Coder<AccumT> accumCoder,
          KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    return keyedCombiningValueInternal(id, accumCoder, combineFn);
  }

  /**
   * Create a state tag for values that use a {@link KeyedCombineFnWithContext} to automatically
   * merge multiple {@code InputT}s into a single {@code OutputT}. The key provided to the
   * {@link KeyedCombineFn} comes from the keyed {@link StateAccessor}, the context provided comes
   * from the {@link StateContext}.
   */
  public static <K, InputT, AccumT, OutputT>
      StateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>>
      keyedCombiningValueWithContext(
          String id,
          Coder<AccumT> accumCoder,
          KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn) {
    return new KeyedCombiningValueWithContextStateTag<K, InputT, AccumT, OutputT>(
        new StructuredId(id),
        accumCoder,
        combineFn);
  }

  /**
   * Create a state tag for values that use a {@link CombineFn} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}.
   *
   * <p>This determines the {@code Coder<AccumT>} from the given {@code Coder<InputT>}, and
   * should only be used to initialize static values.
   */
  public static <InputT, AccumT, OutputT>
      StateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>
      combiningValueFromInputInternal(
          String id, Coder<InputT> inputCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    try {
      Coder<AccumT> accumCoder = combineFn.getAccumulatorCoder(STANDARD_REGISTRY, inputCoder);
      return combiningValueInternal(id, accumCoder, combineFn);
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException(
          "Unable to determine accumulator coder for " + combineFn.getClass().getSimpleName()
          + " from " + inputCoder, e);
    }
  }

  private static <InputT, AccumT,
      OutputT> StateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>
      combiningValueInternal(
      String id, Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return
        new CombiningValueStateTag<InputT, AccumT, OutputT>(
            new StructuredId(id), accumCoder, combineFn);
  }

  private static <K, InputT, AccumT, OutputT>
      StateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> keyedCombiningValueInternal(
          String id,
          Coder<AccumT> accumCoder,
          KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    return new KeyedCombiningValueStateTag<K, InputT, AccumT, OutputT>(
        new StructuredId(id), accumCoder, combineFn);
  }

  /**
   * Create a state tag that is optimized for adding values frequently, and
   * occasionally retrieving all the values that have been added.
   */
  public static <T> StateTag<Object, BagState<T>> bag(String id, Coder<T> elemCoder) {
    return new BagStateTag<T>(new StructuredId(id), elemCoder);
  }

  /**
   * Create a state tag for holding the watermark.
   */
  public static <W extends BoundedWindow> StateTag<Object, WatermarkHoldState<W>>
      watermarkStateInternal(String id, OutputTimeFn<? super W> outputTimeFn) {
    return new WatermarkStateTagInternal<W>(new StructuredId(id), outputTimeFn);
  }

  /**
   * Convert an arbitrary {@link StateTag} to a system-internal tag that is guaranteed not to
   * collide with any user tags.
   */
  public static <K, StateT extends State> StateTag<K, StateT> makeSystemTagInternal(
      StateTag<K, StateT> tag) {
    if (!(tag instanceof SystemStateTag)) {
      throw new IllegalArgumentException("Expected subclass of StateTagBase, got " + tag);
    }
    // Checked above
    @SuppressWarnings("unchecked")
    SystemStateTag<K, StateT> typedTag = (SystemStateTag<K, StateT>) tag;
    return typedTag.asKind(StateKind.SYSTEM);
  }

  public static <K, InputT, AccumT, OutputT> StateTag<Object, BagState<AccumT>>
      convertToBagTagInternal(
          StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> combiningTag) {
    if (combiningTag instanceof KeyedCombiningValueStateTag) {
      // Checked above; conversion to a bag tag depends on the provided tag being one of those
      // created via the factory methods in this class.
      @SuppressWarnings("unchecked")
      KeyedCombiningValueStateTag<K, InputT, AccumT, OutputT> typedTag =
          (KeyedCombiningValueStateTag<K, InputT, AccumT, OutputT>) combiningTag;
      return typedTag.asBagTag();
    } else if (combiningTag instanceof KeyedCombiningValueWithContextStateTag) {
      @SuppressWarnings("unchecked")
      KeyedCombiningValueWithContextStateTag<K, InputT, AccumT, OutputT> typedTag =
          (KeyedCombiningValueWithContextStateTag<K, InputT, AccumT, OutputT>) combiningTag;
      return typedTag.asBagTag();
    } else {
      throw new IllegalArgumentException("Unexpected StateTag " + combiningTag);
    }
  }

  private static class StructuredId implements Serializable {
    private final StateKind kind;
    private final String rawId;

    private StructuredId(String rawId) {
      this(StateKind.USER, rawId);
    }

    private StructuredId(StateKind kind, String rawId) {
      this.kind = kind;
      this.rawId = rawId;
    }

    public StructuredId asKind(StateKind kind) {
      return new StructuredId(kind, rawId);
    }

    public void appendTo(Appendable sb) throws IOException {
      sb.append(kind.prefix).append(rawId);
    }

    public String getRawId() {
      return rawId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", rawId)
          .add("kind", kind)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof StructuredId)) {
        return false;
      }

      StructuredId that = (StructuredId) obj;
      return Objects.equals(this.kind, that.kind)
          && Objects.equals(this.rawId, that.rawId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, rawId);
    }
  }

  /**
   * A base class that just manages the structured ids.
   */
  private abstract static class StateTagBase<K, StateT extends State>
      implements StateTag<K, StateT>, SystemStateTag<K, StateT> {

    protected final StructuredId id;

    protected StateTagBase(StructuredId id) {
      this.id = id;
    }

    @Override
    public String getId() {
      return id.getRawId();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", id)
          .toString();
    }

    @Override
    public void appendTo(Appendable sb) throws IOException {
      id.appendTo(sb);
    }

    @Override
    public abstract StateTag<K, StateT> asKind(StateKind kind);
  }

  /**
   * A value state cell for values of type {@code T}.
   *
   * @param <T> the type of value being stored
   */
  private static class ValueStateTag<T> extends StateTagBase<Object, ValueState<T>>
      implements StateTag<Object, ValueState<T>> {

    private final Coder<T> coder;

    private ValueStateTag(StructuredId id, Coder<T> coder) {
      super(id);
      this.coder = coder;
    }

    @Override
    public ValueState<T> bind(StateBinder<?> visitor) {
      return visitor.bindValue(this, coder);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof ValueStateTag)) {
        return false;
      }

      ValueStateTag<?> that = (ValueStateTag<?>) obj;
      return Objects.equals(this.id, that.id)
          && Objects.equals(this.coder, that.coder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), id, coder);
    }

    @Override
    public StateTag<Object, ValueState<T>> asKind(StateKind kind) {
      return new ValueStateTag<T>(id.asKind(kind), coder);
    }
  }

  /**
   * A state cell for values that are combined according to a {@link CombineFn}.
   *
   * @param <InputT> the type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  private static class CombiningValueStateTag<InputT, AccumT, OutputT>
      extends KeyedCombiningValueStateTag<Object, InputT, AccumT, OutputT>
      implements StateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>,
      SystemStateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> {

    private final Coder<AccumT> accumCoder;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private CombiningValueStateTag(
        StructuredId id,
        Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(id, accumCoder, combineFn.asKeyedFn());
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public StateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>
    asKind(StateKind kind) {
      return new CombiningValueStateTag<InputT, AccumT, OutputT>(
          id.asKind(kind), accumCoder, combineFn);
    }
  }

  /**
   * A state cell for values that are combined according to a {@link KeyedCombineFnWithContext}.
   *
   * @param <K> the type of keys
   * @param <InputT> the type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  private static class KeyedCombiningValueWithContextStateTag<K, InputT, AccumT, OutputT>
    extends StateTagBase<K, AccumulatorCombiningState<InputT, AccumT, OutputT>>
    implements SystemStateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> {

    private final Coder<AccumT> accumCoder;
    private final KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn;

    protected KeyedCombiningValueWithContextStateTag(
        StructuredId id,
        Coder<AccumT> accumCoder,
        KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn) {
      super(id);
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> bind(
        StateBinder<? extends K> visitor) {
      return visitor.bindKeyedCombiningValueWithContext(this, accumCoder, combineFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof KeyedCombiningValueWithContextStateTag)) {
        return false;
      }

      KeyedCombiningValueWithContextStateTag<?, ?, ?, ?> that =
          (KeyedCombiningValueWithContextStateTag<?, ?, ?, ?>) obj;
      return Objects.equals(this.id, that.id)
          && Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), id, accumCoder);
    }

    @Override
    public StateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> asKind(
        StateKind kind) {
      return new KeyedCombiningValueWithContextStateTag<>(
          id.asKind(kind), accumCoder, combineFn);
    }

    private StateTag<Object, BagState<AccumT>> asBagTag() {
      return new BagStateTag<AccumT>(id, accumCoder);
    }
  }

  /**
   * A state cell for values that are combined according to a {@link KeyedCombineFn}.
   *
   * @param <K> the type of keys
   * @param <InputT> the type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  private static class KeyedCombiningValueStateTag<K, InputT, AccumT, OutputT>
      extends StateTagBase<K, AccumulatorCombiningState<InputT, AccumT, OutputT>>
      implements SystemStateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> {

    private final Coder<AccumT> accumCoder;
    private final KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn;

    protected KeyedCombiningValueStateTag(
        StructuredId id,
        Coder<AccumT> accumCoder, KeyedCombineFn<K, InputT, AccumT, OutputT> keyedCombineFn) {
      super(id);
      this.keyedCombineFn = keyedCombineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> bind(
        StateBinder<? extends K> visitor) {
      return visitor.bindKeyedCombiningValue(this, accumCoder, keyedCombineFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof CombiningValueStateTag)) {
        return false;
      }

      KeyedCombiningValueStateTag<?, ?, ?, ?> that = (KeyedCombiningValueStateTag<?, ?, ?, ?>) obj;
      return Objects.equals(this.id, that.id)
          && Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), id, accumCoder);
    }

    @Override
    public StateTag<K, AccumulatorCombiningState<InputT, AccumT, OutputT>> asKind(
        StateKind kind) {
      return new KeyedCombiningValueStateTag<>(id.asKind(kind), accumCoder, keyedCombineFn);
    }

    private StateTag<Object, BagState<AccumT>> asBagTag() {
      return new BagStateTag<AccumT>(id, accumCoder);
    }
  }

  /**
   * A state cell optimized for bag-like access patterns (frequent additions, occasional reads
   * of all the values).
   *
   * @param <T> the type of value in the bag
   */
  private static class BagStateTag<T> extends StateTagBase<Object, BagState<T>>
      implements StateTag<Object, BagState<T>>{

    private final Coder<T> elemCoder;

    private BagStateTag(StructuredId id, Coder<T> elemCoder) {
      super(id);
      this.elemCoder = elemCoder;
    }

    @Override
    public BagState<T> bind(StateBinder<?> visitor) {
      return visitor.bindBag(this, elemCoder);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof BagStateTag)) {
        return false;
      }

      BagStateTag<?> that = (BagStateTag<?>) obj;
      return Objects.equals(this.id, that.id)
          && Objects.equals(this.elemCoder, that.elemCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), id, elemCoder);
    }

    @Override
    public StateTag<Object, BagState<T>> asKind(StateKind kind) {
      return new BagStateTag<>(id.asKind(kind), elemCoder);
    }
  }

  private static class WatermarkStateTagInternal<W extends BoundedWindow>
      extends StateTagBase<Object, WatermarkHoldState<W>> {

    /**
     * When multiple output times are added to hold the watermark, this determines how they are
     * combined, and also the behavior when merging windows. Does not contribute to equality/hash
     * since we have at most one watermark hold tag per computation.
     */
    private final OutputTimeFn<? super W> outputTimeFn;

    private WatermarkStateTagInternal(StructuredId id, OutputTimeFn<? super W> outputTimeFn) {
      super(id);
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public WatermarkHoldState<W> bind(StateBinder<?> visitor) {
      return visitor.bindWatermark(this, outputTimeFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof WatermarkStateTagInternal)) {
        return false;
      }

      WatermarkStateTagInternal<?> that = (WatermarkStateTagInternal<?>) obj;
      return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), id);
    }

    @Override
    public StateTag<Object, WatermarkHoldState<W>> asKind(StateKind kind) {
      return new WatermarkStateTagInternal<W>(id.asKind(kind), outputTimeFn);
    }
  }
}
