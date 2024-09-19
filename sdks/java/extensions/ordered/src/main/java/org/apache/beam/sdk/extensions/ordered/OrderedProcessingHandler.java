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
import org.apache.beam.sdk.extensions.ordered.combiner.DefaultSequenceCombiner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.GloballyAsSingletonView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Parent class for Ordered Processing configuration handlers.
 *
 * @param <EventT>  type of events to be processed
 * @param <KeyT>    type of keys which will be used to group the events
 * @param <StateT>  type of internal State which will be used for processing
 * @param <ResultT> type of the result of the processing which will be output
 */
public abstract class OrderedProcessingHandler<
    EventT, KeyT, StateT extends MutableState<EventT, ?>, ResultT>
    implements Serializable {

  private static final int DEFAULT_STATUS_UPDATE_FREQUENCY_SECONDS = 5;
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

  /**
   * Provide concrete classes which will be used by the ordered processing transform.
   *
   * @param eventTClass  class of the events
   * @param keyTClass    class of the keys
   * @param stateTClass  class of the state
   * @param resultTClass class of the results
   */
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

  /**
   * @return the event examiner instance which will be used by the transform.
   */
  public abstract @NonNull EventExaminer<EventT, StateT> getEventExaminer();

  /**
   * Provide the event coder.
   *
   * <p>The default implementation of the method will use the event coder from the input
   * PCollection. If the input PCollection doesn't use KVCoder, it will attempt to get the coder
   * from the pipeline's coder registry.
   *
   * @param pipeline   of the transform
   * @param inputCoder input coder of the transform
   * @return event coder
   * @throws CannotProvideCoderException if the method can't determine the coder based on the above
   *                                     algorithm.
   */
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

  /**
   * Provide the state coder.
   *
   * <p>The default implementation will attempt to get the coder from the pipeline's code registry.
   *
   * @param pipeline of the transform
   * @return the state coder
   * @throws CannotProvideCoderException
   */
  public @NonNull Coder<StateT> getStateCoder(Pipeline pipeline)
      throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(stateTClass);
  }

  /**
   * Provide the key coder.
   *
   * <p>The default implementation of the method will use the event coder from the input
   * PCollection. If the input PCollection doesn't use KVCoder, it will attempt to get the coder
   * from the pipeline's coder registry.
   *
   * @param pipeline
   * @param inputCoder
   * @return
   * @throws CannotProvideCoderException if the method can't determine the coder based on the above
   *                                     algorithm.
   */
  public @NonNull Coder<KeyT> getKeyCoder(
      Pipeline pipeline, Coder<KV<KeyT, KV<Long, EventT>>> inputCoder)
      throws CannotProvideCoderException {
    if (KvCoder.class.isAssignableFrom(inputCoder.getClass())) {
      return ((KvCoder<KeyT, KV<Long, EventT>>) inputCoder).getKeyCoder();
    }
    return pipeline.getCoderRegistry().getCoder(keyTClass);
  }

  /**
   * Provide the result coder.
   *
   * <p>The default implementation will attempt to get the coder from the pipeline's code registry.
   *
   * @param pipeline
   * @return result coder
   * @throws CannotProvideCoderException
   */
  public @NonNull Coder<ResultT> getResultCoder(Pipeline pipeline)
      throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(resultTClass);
  }

  /**
   * Determines the frequency of emission of the {@link OrderedProcessingStatus} elements.
   *
   * <p>Default is 5 seconds.
   *
   * @return the frequency of updates. If null is returned, no updates will be emitted on a
   * scheduled basis.
   */
  public @Nullable Duration getStatusUpdateFrequency() {
    return statusUpdateFrequency;
  }

  /**
   * Changes the default status update frequency. Updates will be disabled if set to null.
   *
   * @param statusUpdateFrequency
   */
  public void setStatusUpdateFrequency(Duration statusUpdateFrequency) {
    this.statusUpdateFrequency = statusUpdateFrequency;
  }

  /**
   * Indicates if the status update needs to be sent after each event's processing.
   *
   * <p>Default is false.
   *
   * @return
   * @see OrderedProcessingHandler#getStatusUpdateFrequency() getStatusUpdateFrequency
   * @see OrderedEventProcessorResult#processingStatuses() PCollection of processing statuses
   */
  public boolean isProduceStatusUpdateOnEveryEvent() {
    return produceStatusUpdateOnEveryEvent;
  }

  /**
   * Sets the indicator of whether the status notification needs to be produced on every event.
   *
   * @param value
   */
  public void setProduceStatusUpdateOnEveryEvent(boolean value) {
    this.produceStatusUpdateOnEveryEvent = value;
  }

  /**
   * Returns the maximum number of elements which will be output per each bundle. The default is
   * 10,000 elements.
   *
   * <p>This is used to limit the amount of data produced for each bundle - many runners have
   * limitations on how much data can be output from a single bundle. If many events arrive out of
   * sequence and are buffered then at some point a single event can cause processing of a large
   * number of buffered events.
   *
   * @return
   */
  public int getMaxOutputElementsPerBundle() {
    return maxOutputElementsPerBundle;
  }

  /**
   * Overrides the default value.
   *
   * @param maxOutputElementsPerBundle
   */
  public void setMaxOutputElementsPerBundle(int maxOutputElementsPerBundle) {
    this.maxOutputElementsPerBundle = maxOutputElementsPerBundle;
  }

  public abstract static class OrderedProcessingGlobalSequenceHandler<
      EventT, KeyT, StateT extends MutableState<EventT, ?>, ResultT> extends
      OrderedProcessingHandler<EventT, KeyT, StateT, ResultT> {

    public OrderedProcessingGlobalSequenceHandler(
        Class<EventT> eventTClass,
        Class<KeyT> keyTClass,
        Class<StateT> stateTClass,
        Class<ResultT> resultTClass) {
      super(eventTClass, keyTClass, stateTClass, resultTClass);
    }

    public GloballyAsSingletonView<TimestampedValue<KV<KeyT, KV<Long, EventT>>>, CompletedSequenceRange> getGlobalSequenceCombiner() {
      return Combine.globally(
          new DefaultSequenceCombiner<KeyT, EventT, StateT>(getEventExaminer())).asSingletonView();
    }

    public Duration getFrequencyOfCheckingForNewGlobalSequence() {
      return Duration.standardSeconds(1);
    }
  }
}
