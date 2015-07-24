/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.annotations.Experimental.Kind;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Static utility methods for creating {@link StateTag} instances.
 */
@Experimental(Kind.STATE)
public class StateTags {

  private StateTags() { }

  /**
   * Create a simple state tag for values of type {@code T}.
   */
  public static <T> StateTag<ValueState<T>> value(String id, Coder<T> valueCoder) {
    return new ValueStateTag<>(id, valueCoder);
  }

  private abstract static class StateTagBase<StateT extends State> implements StateTag<StateT> {

    private static final long serialVersionUID = 0;

    private final String id;

    protected StateTagBase(String id) {
      this.id = id;
    }

    /**
     * Returns the identifier for this state cell.
     */
    @Override
    public String getId() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass()).add("id", id).toString();
    }
  }

  /**
   * Create a state tag for values that use a {@link CombineFn} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}.
   */
  public static <InputT, OutputT> StateTag<CombiningValueState<InputT, OutputT>>
  combiningValue(String id, Coder<InputT> inputCoder, CombineFn<InputT, ?, OutputT> combineFn) {
    return combiningValueInternal(id, inputCoder, combineFn);
  }

  private static <InputT, AccumT, OutputT> StateTag<CombiningValueState<InputT, OutputT>>
  combiningValueInternal(
      String id, Coder<InputT> inputCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> internal =
        new CombiningValueStateTag<InputT, AccumT, OutputT>(id, inputCoder, combineFn);

    // This is a safe cast, since StateTag only supports reading, and
    // CombiningValue<InputT, OutputT> is a super-interface of
    // CombiningValueInternal<InputT, AccumT, OutputT>
    @SuppressWarnings({"unchecked", "rawtypes"})
    StateTag<CombiningValueState<InputT, OutputT>> external = (StateTag) internal;
    return external;
  }

  /**
   * Create a state tag that is optimized for adding values frequently, and
   * occasionally retrieving all the values that have been added.
   */
  public static <T> StateTag<BagState<T>> bag(String id, Coder<T> elemCoder) {
    return new BagStateTag<T>(id, elemCoder);
  }

  /**
   * Create a state tag for holding the watermark.
   */
  public static <T> StateTag<WatermarkStateInternal> watermarkStateInternal(String id) {
    return new WatermarkStateTagInternal(id);
  }

  /**
   * A value state cell for values of type {@code T}.
   *
   * @param <T> the type of value being stored
   */
  private static class ValueStateTag<T> extends StateTagBase<ValueState<T>> {

    private static final long serialVersionUID = 0;

    private final Coder<T> coder;

    private ValueStateTag(String id, Coder<T> coder) {
      super(id);
      this.coder = coder;
    }

    @Override
    public ValueState<T> bind(StateBinder visitor) {
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
      return Objects.equals(this.getId(), that.getId())
          && Objects.equals(this.coder, that.coder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), getId(), coder);
    }
  }

  /**
   * A general purpose state cell for values of type {@code T}.
   *
   * @param <InputT> the type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  private static class CombiningValueStateTag<InputT, AccumT, OutputT>
      extends StateTagBase<CombiningValueStateInternal<InputT, AccumT, OutputT>> {

    private static final long serialVersionUID = 0;

    // TODO: This should use the CoderRegistry from the running pipelie to ensure that it picks up
    // any custom Coders, but that CoderRegistry isn't currently available on the worker.
    private static final CoderRegistry registry = new CoderRegistry();
    static {
      registry.registerStandardCoders();
    }

    private final Coder<AccumT> accumCoder;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    private CombiningValueStateTag(
        String id, Coder<InputT> inputCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(id);

      try {
        this.accumCoder = combineFn.getAccumulatorCoder(registry, inputCoder);
      } catch (CannotProvideCoderException e) {
        throw new RuntimeException(
            "Unable to determine accumulator coder for combineFn: " + combineFn.getClass(), e);
      }

      this.combineFn = combineFn;
    }

    @Override
    public CombiningValueStateInternal<InputT, AccumT, OutputT> bind(StateBinder visitor) {
      return visitor.bindCombiningValue(this, accumCoder, combineFn);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof CombiningValueStateTag)) {
        return false;
      }

      CombiningValueStateTag<?, ?, ?> that = (CombiningValueStateTag<?, ?, ?>) obj;
      return Objects.equals(this.getId(), that.getId())
          && Objects.equals(this.accumCoder, that.accumCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), getId(), accumCoder);
    }
  }

  /**
   * A state cell optimized for bag-like access patterns (frequent additions, occasional reads
   * of all the values).
   *
   * @param <T> the type of value in the bag
   */
  private static class BagStateTag<T> extends StateTagBase<BagState<T>> {

    private static final long serialVersionUID = 0;

    private final Coder<T> elemCoder;

    private BagStateTag(String id, Coder<T> elemCoder) {
      super(id);
      this.elemCoder = elemCoder;
    }

    @Override
    public BagState<T> bind(StateBinder visitor) {
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
      return Objects.equals(this.getId(), that.getId())
          && Objects.equals(this.elemCoder, that.elemCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), getId(), elemCoder);
    }
  }

  private static class WatermarkStateTagInternal extends StateTagBase<WatermarkStateInternal> {

    private static final long serialVersionUID = 0;

    private WatermarkStateTagInternal(String id) {
      super(id);
    }

    @Override
    public WatermarkStateInternal bind(StateBinder visitor) {
      return visitor.bindWatermark(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof WatermarkStateTagInternal)) {
        return false;
      }

      WatermarkStateTagInternal that = (WatermarkStateTagInternal) obj;
      return Objects.equals(this.getId(), that.getId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), getId());
    }
  }
}
