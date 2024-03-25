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
package org.apache.beam.sdk.extensions.ordered;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public abstract class OrderedProcessingHandler<
        EventT, KeyT, StateT extends MutableState<EventT, ?>, ResultT>
    implements Serializable {

  public static final int DEFAULT_STATUS_UPDATE_FREQUENCY_SECONDS = 5;
  private static final boolean DEFAULT_PRODUCE_STATUS_UPDATE_ON_EVERY_EVENT = false;
  public static final int DEFAULT_MAX_ELEMENTS_TO_OUTPUT = 10_000;

  private final Class<EventT> eventTClass;
  private final Class<KeyT> keyTClass;
  private final Class<StateT> stateTClass;
  private final Class<ResultT> resultTClass;

  private int maxOutputElementsPerBundle = DEFAULT_MAX_ELEMENTS_TO_OUTPUT;
  private Duration statusUpdateFrequency =
      Duration.standardSeconds(DEFAULT_STATUS_UPDATE_FREQUENCY_SECONDS);
  private boolean produceStatusUpdateOnEveryEvent = DEFAULT_PRODUCE_STATUS_UPDATE_ON_EVERY_EVENT;

  public OrderedProcessingHandler(
      Class<EventT> eventTClass,
      Class<KeyT> keyTClass,
      Class<StateT> stateTClass,
      Class<ResultT> resultTClass) {
    this.eventTClass = eventTClass;
    this.keyTClass = keyTClass;
    this.stateTClass = stateTClass;
    this.resultTClass = resultTClass;
  }

  public abstract @NonNull EventExaminer<EventT, StateT> getEventExaminer();

  public @NonNull Coder<EventT> getEventCoder(
      Pipeline pipeline, Coder<KV<KeyT, KV<Long, EventT>>> inputCoder)
      throws CannotProvideCoderException {
    if (KvCoder.class.isAssignableFrom(inputCoder.getClass())) {
      Coder<KV<Long, EventT>> valueCoder =
          ((KvCoder<KeyT, KV<Long, EventT>>) inputCoder).getValueCoder();
      if (KV.class.isAssignableFrom(valueCoder.getClass())) {
        return ((KvCoder<Long, EventT>) valueCoder).getValueCoder();
      }
    }
    return pipeline.getCoderRegistry().getCoder(eventTClass);
  }

  public Coder<StateT> getStateCoder(Pipeline pipeline) throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(stateTClass);
  }

  public Coder<KeyT> getKeyCoder(Pipeline pipeline, Coder<KV<KeyT, KV<Long, EventT>>> inputCoder)
      throws CannotProvideCoderException {
    if (KvCoder.class.isAssignableFrom(inputCoder.getClass())) {
      return ((KvCoder<KeyT, KV<Long, EventT>>) inputCoder).getKeyCoder();
    }
    return pipeline.getCoderRegistry().getCoder(keyTClass);
  }

  public Coder<ResultT> getResultCoder(Pipeline pipeline) throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(resultTClass);
  }

  public Duration getStatusUpdateFrequency() {
    return statusUpdateFrequency;
  }

  public void setStatusUpdateFrequency(Duration statusUpdateFrequency) {
    this.statusUpdateFrequency = statusUpdateFrequency;
  }

  public boolean isProduceStatusUpdateOnEveryEvent() {
    return produceStatusUpdateOnEveryEvent;
  }

  public int getMaxOutputElementsPerBundle() {
    return maxOutputElementsPerBundle;
  }

  public void setMaxOutputElementsPerBundle(int maxOutputElementsPerBundle) {
    this.maxOutputElementsPerBundle = maxOutputElementsPerBundle;
  }

  public void setProduceStatusUpdateOnEveryEvent(boolean value) {
    this.produceStatusUpdateOnEveryEvent = value;
  }
}
