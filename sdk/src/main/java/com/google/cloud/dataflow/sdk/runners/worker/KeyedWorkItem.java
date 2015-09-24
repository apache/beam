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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.InputMessageBundle;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.Message;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.Timer;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around a {@link Windmill.WorkItem} which contains all the timers and elements associated
 * with a specific work item.
 *
 * <p>Used as the input type of {@link StreamingGroupAlsoByWindowsDoFn}.
 *
 * @param <ElemT> the element type
 */
public class KeyedWorkItem<ElemT> {

  private static final Predicate<Timer> IS_WATERMARK = new Predicate<Timer>() {
    @Override
    public boolean apply(Timer input) {
      return input.getType() == Timer.Type.WATERMARK;
    }
  };

  public static <ElemT> KeyedWorkItem<ElemT> workItem(
      Object key,
      Windmill.WorkItem workItem,
      Coder<? extends BoundedWindow> windowCoder,
      Coder<Collection<? extends BoundedWindow>> windowsCoder,
      Coder<ElemT> valueCoder) {
    return new KeyedWorkItem<>(key, workItem, windowCoder, windowsCoder, valueCoder);
  }

  public Object key() {
    return key;
  }

  public Iterable<TimerData> timersIterable() {
    FluentIterable<Timer> allTimers = FluentIterable.from(workItem.getTimers().getTimersList());
    FluentIterable<Timer> eventTimers = allTimers.filter(IS_WATERMARK);
    FluentIterable<Timer> nonEventTimers = allTimers.filter(Predicates.not(IS_WATERMARK));
    return eventTimers.append(nonEventTimers).transform(new Function<Timer, TimerData>() {
      private TimeDomain getTimeDomain(Windmill.Timer.Type type) {
        switch (type) {
          case REALTIME:
            return TimeDomain.PROCESSING_TIME;
          case DEPENDENT_REALTIME:
            return TimeDomain.SYNCHRONIZED_PROCESSING_TIME;
          case WATERMARK:
            return TimeDomain.EVENT_TIME;
          default:
            throw new IllegalArgumentException("Unsupported timer type " + type);
        }
      }

      @Override
      public TimerData apply(Timer timer) {
        String tag = timer.getTag().toStringUtf8();
        String namespaceString = tag.substring(0, tag.indexOf('+'));
        StateNamespace namespace = StateNamespaces.fromString(namespaceString, windowCoder);

        Instant timestamp = new Instant(TimeUnit.MICROSECONDS.toMillis(timer.getTimestamp()));
        return TimerData.of(namespace, timestamp, getTimeDomain(timer.getType()));
      }
    });
  }

  public Iterable<WindowedValue<ElemT>> elementsIterable() {
    return FluentIterable.from(workItem.getMessageBundlesList())
        .transformAndConcat(new Function<InputMessageBundle, Iterable<Message>>() {
          @Override
          public Iterable<Message> apply(InputMessageBundle input) {
            return input.getMessagesList();
          }
        })
        .transform(new Function<Message, WindowedValue<ElemT>>() {
          @Override
          public WindowedValue<ElemT> apply(Message message) {
            try {
              Instant timestamp = new Instant(
                  TimeUnit.MICROSECONDS.toMillis(message.getTimestamp()));
              Collection<? extends BoundedWindow> windows =
                  WindmillSink.decodeMetadataWindows(windowsCoder, message.getMetadata());
              PaneInfo pane = WindmillSink.decodeMetadataPane(message.getMetadata());

              InputStream inputStream = message.getData().newInput();
              ElemT value = valueCoder.decode(inputStream, Coder.Context.OUTER);
              return WindowedValue.of(value, timestamp, windows, pane);
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        });
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KeyedWorkItem)) {
      return false;
    }

    KeyedWorkItem<?> that = (KeyedWorkItem<?>) other;
    return Objects.equals(this.key, that.key)
        && Objects.equals(this.workItem, that.workItem);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, workItem);
  }

  /**
   * Coder that forwards {@code ByteSizeObserver} calls to an underlying element coder.
   * {@code TimerOrElement} objects never need to be encoded, so this class does not
   * support the {@code encode} and {@code decode} methods.
   */
  public static class KeyedWorkItemCoder<T> extends StandardCoder<KeyedWorkItem<T>> {
    final Coder<T> elemCoder;

    /**
     * Creates a new {@code TimerOrElement.Coder} that wraps the given {@link Coder}.
     */
    public static <T> KeyedWorkItemCoder<T> of(Coder<T> elemCoder) {
      return new KeyedWorkItemCoder<>(elemCoder);
    }

    @JsonCreator
    public static KeyedWorkItemCoder<?> of(
            @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
            List<Object> components) {
      return of((Coder<?>) components.get(0));
    }

    @Override
    public void encode(KeyedWorkItem<T> value, OutputStream outStream, Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public KeyedWorkItem<T> decode(InputStream inStream, Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(KeyedWorkItem<T> value, Context context) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        KeyedWorkItem<T> value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      observer.update((long) value.workItem.getSerializedSize());
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(elemCoder);
    }

    public Coder<T> getElementCoder() {
      return elemCoder;
    }

    protected KeyedWorkItemCoder(Coder<T> elemCoder) {
      this.elemCoder = elemCoder;
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  private final Windmill.WorkItem workItem;
  private final Object key;

  private final transient Coder<? extends BoundedWindow> windowCoder;
  private final transient Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private final transient Coder<ElemT> valueCoder;

  KeyedWorkItem(
      Object key,
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
}
