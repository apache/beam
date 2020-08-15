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
package org.apache.beam.runners.core;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Static utility methods for creating {@link StateTag} instances. */
public class StateTags {

  private static final CoderRegistry STANDARD_REGISTRY = CoderRegistry.createDefault();

  public static final Equivalence<StateTag> ID_EQUIVALENCE =
      new Equivalence<StateTag>() {
        @Override
        protected boolean doEquivalent(StateTag a, StateTag b) {
          return a.getId().equals(b.getId());
        }

        @Override
        protected int doHash(StateTag stateTag) {
          return stateTag.getId().hashCode();
        }
      };

  /** @deprecated for migration purposes only */
  @Deprecated
  private static StateBinder adaptTagBinder(final StateTag.StateBinder binder) {
    return new StateBinder() {
      @Override
      public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
        return binder.bindValue(tagForSpec(id, spec), coder);
      }

      @Override
      public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
        return binder.bindBag(tagForSpec(id, spec), elemCoder);
      }

      @Override
      public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
        return binder.bindSet(tagForSpec(id, spec), elemCoder);
      }

      @Override
      public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
          String id,
          StateSpec<MapState<KeyT, ValueT>> spec,
          Coder<KeyT> mapKeyCoder,
          Coder<ValueT> mapValueCoder) {
        return binder.bindMap(tagForSpec(id, spec), mapKeyCoder, mapValueCoder);
      }

      @Override
      public <T> OrderedListState<T> bindOrderedList(
          String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
        return binder.bindOrderedList(tagForSpec(id, spec), elemCoder);
      }

      @Override
      public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
          String id,
          StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
          Coder<AccumT> accumCoder,
          CombineFn<InputT, AccumT, OutputT> combineFn) {
        return binder.bindCombiningValue(tagForSpec(id, spec), accumCoder, combineFn);
      }

      @Override
      public <InputT, AccumT, OutputT>
          CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        return binder.bindCombiningValueWithContext(tagForSpec(id, spec), accumCoder, combineFn);
      }

      @Override
      public WatermarkHoldState bindWatermark(
          String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
        return binder.bindWatermark(tagForSpec(id, spec), timestampCombiner);
      }
    };
  }

  private enum StateKind {
    SYSTEM('s'),
    USER('u');

    private final char prefix;

    StateKind(char prefix) {
      this.prefix = prefix;
    }
  }

  private StateTags() {}

  private interface SystemStateTag<StateT extends State> {
    StateTag<StateT> asKind(StateKind kind);
  }

  /** Create a state tag for the given id and spec. */
  public static <StateT extends State> StateTag<StateT> tagForSpec(
      String id, StateSpec<StateT> spec) {
    return new SimpleStateTag<>(new StructuredId(id), spec);
  }

  /** Create a simple state tag for values of type {@code T}. */
  public static <T> StateTag<ValueState<T>> value(String id, Coder<T> valueCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.value(valueCoder));
  }

  /**
   * Create a state tag for values that use a {@link CombineFn} to automatically merge multiple
   * {@code InputT}s into a single {@code OutputT}.
   */
  public static <InputT, AccumT, OutputT>
      StateTag<CombiningState<InputT, AccumT, OutputT>> combiningValue(
          String id, Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.combining(accumCoder, combineFn));
  }

  /**
   * Create a state tag for values that use a {@link CombineFnWithContext} to automatically merge
   * multiple {@code InputT}s into a single {@code OutputT}.
   */
  public static <InputT, AccumT, OutputT>
      StateTag<CombiningState<InputT, AccumT, OutputT>> combiningValueWithContext(
          String id,
          Coder<AccumT> accumCoder,
          CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.combining(accumCoder, combineFn));
  }

  /**
   * Create a state tag for values that use a {@link CombineFn} to automatically merge multiple
   * {@code InputT}s into a single {@code OutputT}.
   *
   * <p>This determines the {@code Coder<AccumT>} from the given {@code Coder<InputT>}, and should
   * only be used to initialize static values.
   */
  public static <InputT, AccumT, OutputT>
      StateTag<CombiningState<InputT, AccumT, OutputT>> combiningValueFromInputInternal(
          String id, Coder<InputT> inputCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.combiningFromInputInternal(inputCoder, combineFn));
  }

  /**
   * Create a state tag that is optimized for adding values frequently, and occasionally retrieving
   * all the values that have been added.
   */
  public static <T> StateTag<BagState<T>> bag(String id, Coder<T> elemCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.bag(elemCoder));
  }

  /** Create a state spec that supporting for {@link java.util.Set} like access patterns. */
  public static <T> StateTag<SetState<T>> set(String id, Coder<T> elemCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.set(elemCoder));
  }

  /** Create a state spec that supporting for {@link java.util.Map} like access patterns. */
  public static <K, V> StateTag<MapState<K, V>> map(
      String id, Coder<K> keyCoder, Coder<V> valueCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.map(keyCoder, valueCoder));
  }

  public static <T> StateTag<OrderedListState<T>> orderedList(String id, Coder<T> elemCoder) {
    return new SimpleStateTag<>(new StructuredId(id), StateSpecs.orderedList(elemCoder));
  }

  /** Create a state tag for holding the watermark. */
  public static <W extends BoundedWindow> StateTag<WatermarkHoldState> watermarkStateInternal(
      String id, TimestampCombiner timestampCombiner) {
    return new SimpleStateTag<>(
        new StructuredId(id), StateSpecs.watermarkStateInternal(timestampCombiner));
  }

  /**
   * Convert an arbitrary {@link StateTag} to a system-internal tag that is guaranteed not to
   * collide with any user tags.
   */
  public static <StateT extends State> StateTag<StateT> makeSystemTagInternal(
      StateTag<StateT> tag) {
    if (!(tag instanceof SystemStateTag)) {
      throw new IllegalArgumentException("Expected subclass of SimpleStateTag, got " + tag);
    }
    // Checked above
    @SuppressWarnings("unchecked")
    SystemStateTag<StateT> typedTag = (SystemStateTag<StateT>) tag;
    return typedTag.asKind(StateKind.SYSTEM);
  }

  public static <InputT, AccumT, OutputT> StateTag<BagState<AccumT>> convertToBagTagInternal(
      StateTag<CombiningState<InputT, AccumT, OutputT>> combiningTag) {
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
      return MoreObjects.toStringHelper(getClass()).add("id", rawId).add("kind", kind).toString();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof StructuredId)) {
        return false;
      }

      StructuredId that = (StructuredId) obj;
      return Objects.equals(this.kind, that.kind) && Objects.equals(this.rawId, that.rawId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, rawId);
    }
  }

  /** A basic {@link StateTag} implementation that manages the structured ids. */
  private static class SimpleStateTag<StateT extends State>
      implements StateTag<StateT>, SystemStateTag<StateT> {

    private final StateSpec<StateT> spec;
    private final StructuredId id;

    public SimpleStateTag(StructuredId id, StateSpec<StateT> spec) {
      this.id = id;
      this.spec = spec;
    }

    /** @deprecated use {@link StateSpec#bind} method via {@link #getSpec} for now. */
    @Override
    @Deprecated
    public StateT bind(StateTag.StateBinder binder) {
      return spec.bind(this.id.getRawId(), adaptTagBinder(binder));
    }

    @Override
    public String getId() {
      return id.getRawId();
    }

    @Override
    public StateSpec<StateT> getSpec() {
      return spec;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass()).add("id", id).toString();
    }

    @Override
    public void appendTo(Appendable sb) throws IOException {
      id.appendTo(sb);
    }

    @Override
    public StateTag<StateT> asKind(StateKind kind) {
      return new SimpleStateTag<>(id.asKind(kind), spec);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof SimpleStateTag)) {
        return false;
      }

      SimpleStateTag<?> otherTag = (SimpleStateTag<?>) other;
      return Objects.equals(this.getId(), otherTag.getId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), this.getId());
    }
  }
}
