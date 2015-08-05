/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.TimerOrElement;
import com.google.cloud.dataflow.sdk.util.TimerOrElement.TimerOrElementCoder;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A Reader that receives input data from a Windmill server, and returns it as
 * groups of elements and timers.
 */
class WindowingWindmillReader<T> extends Reader<WindowedValue<TimerOrElement<T>>> {
  private final Coder<T> valueCoder;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final Coder<Collection<? extends BoundedWindow>> windowsCoder;
  private StreamingModeExecutionContext context;

  WindowingWindmillReader(Coder<WindowedValue<TimerOrElement<T>>> coder,
                          StreamingModeExecutionContext context) {
    FullWindowedValueCoder<TimerOrElement<T>> inputCoder =
        (FullWindowedValueCoder<TimerOrElement<T>>) coder;
    this.windowsCoder = inputCoder.getWindowsCoder();
    this.windowCoder = inputCoder.getWindowCoder();
    this.valueCoder = ((TimerOrElementCoder<T>) inputCoder.getValueCoder()).getElementCoder();
    this.context = context;
  }

  public static <T> WindowingWindmillReader<T> create(PipelineOptions options,
                                             CloudObject spec,
                                             Coder coder,
                                             ExecutionContext context) {
    return new WindowingWindmillReader<>(coder, (StreamingModeExecutionContext) context);
  }

  @Override
  public ReaderIterator<WindowedValue<TimerOrElement<T>>> iterator() throws IOException {
    return new WindowingWindmillReaderIterator();
  }

  class WindowingWindmillReaderIterator
  extends AbstractReaderIterator<WindowedValue<TimerOrElement<T>>> {
    private int bundleIndex = 0;
    private int messageIndex = 0;
    private int processingTimeTimerIndex = 0;
    private int eventTimeTimerIndex = 0;
    Object key = null;
    private List<WindowedValue<TimerOrElement<T>>> eventTimeTimers;
    private List<WindowedValue<TimerOrElement<T>>> processingTimeTimers;

    private WindowingWindmillReaderIterator() throws IOException {
      if (valueCoder instanceof KvCoder) {
        key = ((KvCoder) valueCoder).getKeyCoder().decode(
            context.getSerializedKey().newInput(), Coder.Context.OUTER);
      }

      eventTimeTimers = new ArrayList<>();
      processingTimeTimers = new ArrayList<>();
      for (Windmill.Timer rawTimer : context.getWork().getTimers().getTimersList()) {
        WindowedValue<TimerOrElement<T>> timer = createTimer(rawTimer);
        if (timer.getValue().getTimer().getDomain() == TimeDomain.EVENT_TIME) {
          eventTimeTimers.add(timer);
        } else {
          processingTimeTimers.add(timer);
        }
      }
    }

    private boolean hasMoreMessages() {
      Windmill.WorkItem work = context.getWork();
      return bundleIndex < work.getMessageBundlesCount() &&
          messageIndex < work.getMessageBundles(bundleIndex).getMessagesCount();
    }

    private boolean hasMoreProcessingTimeTimers() {
      return processingTimeTimerIndex < processingTimeTimers.size();
    }

    private boolean hasMoreEventTimeTimers() {
      return eventTimeTimerIndex < eventTimeTimers.size();
    }

    @Override
    public boolean hasNext() throws IOException {
      return hasMoreMessages() || hasMoreProcessingTimeTimers() || hasMoreEventTimeTimers();
    }

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

    private <W extends BoundedWindow> WindowedValue<TimerOrElement<T>> createTimer(
        Windmill.Timer timer) {
      String tag = timer.getTag().toStringUtf8();
      String namespaceString = tag.substring(0, tag.indexOf('+'));
      StateNamespace namespace = StateNamespaces.fromString(namespaceString, windowCoder);

      Instant timestamp = new Instant(TimeUnit.MICROSECONDS.toMillis(timer.getTimestamp()));
      TimerData timerData = TimerData.of(namespace, timestamp, getTimeDomain(timer.getType()));

      return WindowedValue.<TimerOrElement<T>>of(
          TimerOrElement.<T>timer(key, timerData),
          timestamp,
          new ArrayList<W>(),
          PaneInfo.NO_FIRING);
    }

    @Override
    public WindowedValue<TimerOrElement<T>> next() throws IOException {
      if (hasMoreEventTimeTimers()) {
        if (valueCoder instanceof KvCoder) {
          return eventTimeTimers.get(eventTimeTimerIndex++);
        } else {
          throw new RuntimeException("Timer set on non-keyed DoFn");
        }
      } else if (hasMoreProcessingTimeTimers()) {
        if (valueCoder instanceof KvCoder) {
          return processingTimeTimers.get(processingTimeTimerIndex++);
        } else {
          throw new RuntimeException("Timer set on non-keyed DoFn");
        }
      } else {
        Windmill.Message message =
            context.getWork().getMessageBundles(bundleIndex).getMessages(messageIndex);

        if (messageIndex >=
            context.getWork().getMessageBundles(bundleIndex).getMessagesCount() - 1) {
          messageIndex = 0;
          bundleIndex++;
        } else {
          messageIndex++;
        }
        Instant timestampMillis =
            new Instant(TimeUnit.MICROSECONDS.toMillis(message.getTimestamp()));
        InputStream data = message.getData().newInput();
        InputStream metadata = message.getMetadata().newInput();
        Collection<? extends BoundedWindow> windows = WindmillSink.decodeMetadataWindows(
            windowsCoder, message.getMetadata());
        PaneInfo pane = WindmillSink.decodeMetadataPane(message.getMetadata());
        if (valueCoder instanceof KvCoder) {
          KvCoder kvCoder = (KvCoder) valueCoder;
          notifyElementRead(
              context.getSerializedKey().size() + data.available() + metadata.available());
          return WindowedValue.of(
              TimerOrElement.element((T) KV.of(key, decode(kvCoder.getValueCoder(), data))),
              timestampMillis, windows, pane);
        } else {
          notifyElementRead(data.available() + metadata.available());
          return WindowedValue.of(TimerOrElement.element(decode(valueCoder, data)),
                                  timestampMillis,
                                  windows,
                                  pane);
        }
      }
    }

    private <T> T decode(Coder<T> coder, InputStream input) throws IOException {
      return coder.decode(input, Coder.Context.OUTER);
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
