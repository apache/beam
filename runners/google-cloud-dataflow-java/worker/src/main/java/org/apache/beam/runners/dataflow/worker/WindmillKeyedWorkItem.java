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
package org.apache.beam.runners.dataflow.worker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * An implementation of {@link KeyedWorkItem} that wraps around a {@code Windmill.WorkItem}.
 *
 * @param <K> the key type
 * @param <ElemT> the element type
 */
public class WindmillKeyedWorkItem<K, ElemT> implements KeyedWorkItem<K, ElemT> {
  private static final Predicate<Timer> IS_WATERMARK =
      input -> input.getType() == Timer.Type.WATERMARK;

  private final Windmill.WorkItem workItem;
  private final K key;

  private final transient Coder<? extends BoundedWindow> windowCoder;
  private final transient Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private final transient Coder<ElemT> valueCoder;

  public WindmillKeyedWorkItem(
      K key,
      Windmill.WorkItem workItem,
      Coder<? extends BoundedWindow> windowCoder,
      Coder<Collection<? extends BoundedWindow>> windowsCoder,
      Coder<ElemT> valueCoder) {
    this.key = key;
    this.workItem = workItem;
    this.windowCoder = windowCoder;
    this.windowsCoder = windowsCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public K key() {
    return key;
  }

  @Override
  public Iterable<TimerData> timersIterable() {
    FluentIterable<Timer> allTimers = FluentIterable.from(workItem.getTimers().getTimersList());
    FluentIterable<Timer> eventTimers = allTimers.filter(IS_WATERMARK);
    FluentIterable<Timer> nonEventTimers = allTimers.filter(Predicates.not(IS_WATERMARK));
    return eventTimers
        .append(nonEventTimers)
        .transform(
            timer ->
                WindmillTimerInternals.windmillTimerToTimerData(
                    WindmillNamespacePrefix.SYSTEM_NAMESPACE_PREFIX, timer, windowCoder));
  }

  @Override
  public Iterable<WindowedValue<ElemT>> elementsIterable() {
    return FluentIterable.from(workItem.getMessageBundlesList())
        .transformAndConcat(Windmill.InputMessageBundle::getMessagesList)
        .transform(
            message -> {
              try {
                Instant timestamp =
                    WindmillTimeUtils.windmillToHarnessTimestamp(message.getTimestamp());
                Collection<? extends BoundedWindow> windows =
                    WindmillSink.decodeMetadataWindows(windowsCoder, message.getMetadata());
                PaneInfo pane = WindmillSink.decodeMetadataPane(message.getMetadata());

                InputStream inputStream = message.getData().newInput();
                ElemT value = valueCoder.decode(inputStream, Coder.Context.OUTER);
                return WindowedValue.of(value, timestamp, windows, pane);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof WindmillKeyedWorkItem)) {
      return false;
    }

    WindmillKeyedWorkItem<?, ?> that = (WindmillKeyedWorkItem<?, ?>) other;
    return Objects.equals(this.key, that.key) && Objects.equals(this.workItem, that.workItem);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, workItem);
  }

  @Override
  public String toString() {
    return "WindmillKeyedWorkItem{"
        + "key="
        + key()
        + ", elements="
        + Lists.newArrayList(elementsIterable())
        + ", timers="
        + Lists.newArrayList(timersIterable())
        + ", windowCoder="
        + windowCoder
        + ", windowsCoder="
        + windowsCoder
        + ", valueCoder="
        + valueCoder
        + '}';
  }

  /**
   * Coder that forwards {@code ByteSizeObserver} calls to an underlying element coder. {@code
   * TimerOrElement} objects never need to be encoded, so this class does not support the {@code
   * encode} and {@code decode} methods.
   */
  public static class FakeKeyedWorkItemCoder<K, ElemT>
      extends StructuredCoder<KeyedWorkItem<K, ElemT>> {
    final KvCoder<K, ElemT> kvCoder;

    /** Creates a new {@code TimerOrElement.Coder} that wraps the given {@link Coder}. */
    public static <T> FakeKeyedWorkItemCoder<?, ?> of(Coder<T> elemCoder) {
      return new FakeKeyedWorkItemCoder<>(elemCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(kvCoder);
    }

    @Override
    public void encode(KeyedWorkItem<K, ElemT> value, OutputStream outStream) {
      throw new UnsupportedOperationException();
    }

    @Override
    public KeyedWorkItem<K, ElemT> decode(InputStream inStream) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(KeyedWorkItem<K, ElemT> value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        KeyedWorkItem<K, ElemT> value, ElementByteSizeObserver observer) throws Exception {
      if (value instanceof WindmillKeyedWorkItem) {
        long serializedSize = ((WindmillKeyedWorkItem<?, ?>) value).workItem.getSerializedSize();
        observer.update(serializedSize);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    public Coder<K> getKeyCoder() {
      return kvCoder.getKeyCoder();
    }

    public Coder<ElemT> getElementCoder() {
      return kvCoder.getValueCoder();
    }

    protected FakeKeyedWorkItemCoder(Coder<?> elemCoder) {
      if (elemCoder instanceof KeyedWorkItemCoder) {
        KeyedWorkItemCoder kwiCoder = (KeyedWorkItemCoder) elemCoder;
        this.kvCoder = KvCoder.of(kwiCoder.getKeyCoder(), kwiCoder.getElementCoder());
      } else if (elemCoder instanceof KvCoder) {
        this.kvCoder = ((KvCoder) elemCoder);
      } else {
        throw new IllegalArgumentException(
            "FakeKeyedWorkItemCoder only works with KeyedWorkItemCoder or KvCoder; was: "
                + elemCoder.getClass());
      }
    }
  }
}
