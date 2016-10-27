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

import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;

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

  /** Create a state tag for the given id and spec. */
  public static <K, StateT extends State> StateTag<K, StateT> tagForSpec(
      String id, StateSpec<K, StateT> spec) {
    return new SimpleStateTag<>(new StructuredId(id), spec);
  }

  /**
   * Create a simple state tag for values of type {@code T}.
   */
  public static <T> StateTag<Object, ValueState<T>> value(String id, Coder<T> valueCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.value(valueCoder));
  }

  /**
   * Create a state tag for values that use a {@link CombineFn} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}.
   */
  public static <InputT, AccumT, OutputT>
    StateTag<Object, AccumulatorCombiningState<InputT, AccumT, OutputT>>
    combiningValue(
      String id, Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.combiningValue(accumCoder, combineFn));
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
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.keyedCombiningValue(accumCoder, combineFn));
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
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.keyedCombiningValueWithContext(accumCoder, combineFn));
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
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.combiningValueFromInputInternal(inputCoder, combineFn));
  }

  /**
   * Create a state tag that is optimized for adding values frequently, and
   * occasionally retrieving all the values that have been added.
   */
  public static <T> StateTag<Object, BagState<T>> bag(String id, Coder<T> elemCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.bag(elemCoder));
  }

  /**
   * Create a state tag for holding the watermark.
   */
  public static <W extends BoundedWindow> StateTag<Object, WatermarkHoldState<W>>
      watermarkStateInternal(String id, OutputTimeFn<? super W> outputTimeFn) {
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.watermarkStateInternal(outputTimeFn));
  }

  /**
   * Convert an arbitrary {@link StateTag} to a system-internal tag that is guaranteed not to
   * collide with any user tags.
   */
  public static <K, StateT extends State> StateTag<K, StateT> makeSystemTagInternal(
      StateTag<K, StateT> tag) {
    if (!(tag instanceof SystemStateTag)) {
      throw new IllegalArgumentException("Expected subclass of SimpleStateTag, got " + tag);
    }
    // Checked above
    @SuppressWarnings("unchecked")
    SystemStateTag<K, StateT> typedTag = (SystemStateTag<K, StateT>) tag;
    return typedTag.asKind(StateKind.SYSTEM);
  }

  public static <K, InputT, AccumT, OutputT> StateTag<Object, BagState<AccumT>>
      convertToBagTagInternal(
          StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> combiningTag) {
    return new SimpleStateTag<>(
        new StructuredId(combiningTag.getId()),
        StateSpecs.convertToBagSpecInternal(combiningTag.getSpec()));
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
   * A basic {@link StateTag} implementation that manages the structured ids.
   */
  private static class SimpleStateTag<K, StateT extends State>
      implements StateTag<K, StateT>, SystemStateTag<K, StateT> {

    private final StateSpec<K, StateT> spec;
    private final StructuredId id;

    public SimpleStateTag(StructuredId id, StateSpec<K, StateT> spec) {
      this.id = id;
      this.spec = spec;
    }

    @Override
    @Deprecated
    public StateT bind(StateTag.StateBinder<? extends K> binder) {
      return spec.bind(
          this.id.getRawId(), StateSpecs.adaptTagBinder(binder));
    }

    @Override
    public String getId() {
      return id.getRawId();
    }

    @Override
    public StateSpec<K, StateT> getSpec() {
      return spec;
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
    public StateTag<K, StateT> asKind(StateKind kind) {
      return new SimpleStateTag<>(id.asKind(kind), spec);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SimpleStateTag)) {
        return false;
      }

      SimpleStateTag<?, ?> otherTag = (SimpleStateTag<?, ?>) other;
      return Objects.equals(this.getId(), otherTag.getId())
          && Objects.equals(this.getSpec(), otherTag.getSpec());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), this.getId(), this.getSpec());
    }
  }
}
