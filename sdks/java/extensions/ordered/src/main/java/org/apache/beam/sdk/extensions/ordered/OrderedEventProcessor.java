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

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingHandler.OrderedProcessingGlobalSequenceHandler;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.UnprocessedEventCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

/**
 * Transform for processing ordered events. Events are grouped by the key and within each key they
 * are applied according to the provided sequence. Events which arrive out of sequence are buffered
 * and processed after all the missing events for a given key have arrived.
 *
 * <p>There are two sequencing modes - a sequence per key and a global sequence. See {@link
 * OrderedProcessingHandler} for details on how to configure this transform.
 *
 * @param <EventT> type of event
 * @param <EventKeyT> type of event key
 * @param <StateT> type of the state
 */
@AutoValue
@SuppressWarnings({"nullness", "TypeNameShadowing"})
public abstract class OrderedEventProcessor<
        EventT, EventKeyT, ResultT, StateT extends MutableState<EventT, ResultT>>
    extends PTransform<
        PCollection<KV<EventKeyT, KV<Long, EventT>>>,
        OrderedEventProcessorResult<EventKeyT, ResultT, EventT>> {

  public static final String GLOBAL_SEQUENCE_TRACKER = "global_sequence_tracker";

  /**
   * Create the transform.
   *
   * @param handler provides the configuration of this transform
   * @param <EventTypeT> type of event
   * @param <EventKeyTypeT> type of event key
   * @param <ResultTypeT> type of the result object
   * @param <StateTypeT> type of the state to store
   * @return the transform
   */
  public static <
          EventTypeT,
          EventKeyTypeT,
          ResultTypeT,
          StateTypeT extends MutableState<EventTypeT, ResultTypeT>>
      OrderedEventProcessor<EventTypeT, EventKeyTypeT, ResultTypeT, StateTypeT> create(
          OrderedProcessingHandler<EventTypeT, EventKeyTypeT, StateTypeT, ResultTypeT> handler) {
    return new AutoValue_OrderedEventProcessor<>(handler);
  }

  @Nullable
  abstract OrderedProcessingHandler<EventT, EventKeyT, StateT, ResultT> getHandler();

  @Override
  public OrderedEventProcessorResult<EventKeyT, ResultT, EventT> expand(
      PCollection<KV<EventKeyT, KV<Long, EventT>>> input) {
    final TupleTag<KV<EventKeyT, ResultT>> mainOutput =
        new TupleTag<KV<EventKeyT, ResultT>>("mainOutput") {};
    final TupleTag<KV<EventKeyT, OrderedProcessingStatus>> statusOutput =
        new TupleTag<KV<EventKeyT, OrderedProcessingStatus>>("status") {};

    final TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>> unprocessedEventOutput =
        new TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>>("unprocessed-events") {};

    OrderedProcessingHandler<EventT, EventKeyT, StateT, ResultT> handler = getHandler();
    Pipeline pipeline = input.getPipeline();

    Coder<EventKeyT> keyCoder;
    try {
      keyCoder = handler.getKeyCoder(pipeline, input.getCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get key coder", e);
    }

    Coder<EventT> eventCoder;
    try {
      eventCoder = handler.getEventCoder(pipeline, input.getCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get event coder", e);
    }

    Coder<StateT> stateCoder;
    try {
      stateCoder = handler.getStateCoder(pipeline);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get state coder", e);
    }

    Coder<ResultT> resultCoder;
    try {
      resultCoder = handler.getResultCoder(pipeline);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get result coder", e);
    }

    KvCoder<EventKeyT, ResultT> mainOutputCoder = KvCoder.of(keyCoder, resultCoder);
    KvCoder<EventKeyT, OrderedProcessingStatus> processingStatusCoder =
        KvCoder.of(keyCoder, getOrderedProcessingStatusCoder(pipeline));
    KvCoder<EventKeyT, KV<Long, UnprocessedEvent<EventT>>> unprocessedEventsCoder =
        KvCoder.of(
            keyCoder, KvCoder.of(VarLongCoder.of(), new UnprocessedEventCoder<>(eventCoder)));

    if (handler instanceof OrderedProcessingGlobalSequenceHandler) {
      OrderedProcessingGlobalSequenceHandler<EventT, EventKeyT, StateT, ResultT>
          globalSequenceHandler =
              (OrderedProcessingGlobalSequenceHandler<EventT, EventKeyT, StateT, ResultT>) handler;

      return expandGlobalSequenceProcessing(
          input,
          mainOutput,
          statusOutput,
          unprocessedEventOutput,
          handler,
          pipeline,
          keyCoder,
          eventCoder,
          stateCoder,
          mainOutputCoder,
          processingStatusCoder,
          unprocessedEventsCoder,
          globalSequenceHandler);
    } else {
      return expandPerKeyProcessing(
          input,
          mainOutput,
          statusOutput,
          unprocessedEventOutput,
          handler,
          pipeline,
          keyCoder,
          eventCoder,
          stateCoder,
          mainOutputCoder,
          processingStatusCoder,
          unprocessedEventsCoder);
    }
  }

  private OrderedEventProcessorResult<EventKeyT, ResultT, EventT> expandPerKeyProcessing(
      PCollection<KV<EventKeyT, KV<Long, EventT>>> input,
      TupleTag<KV<EventKeyT, ResultT>> mainOutput,
      TupleTag<KV<EventKeyT, OrderedProcessingStatus>> statusOutput,
      TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>> unprocessedEventOutput,
      OrderedProcessingHandler<EventT, EventKeyT, StateT, ResultT> handler,
      Pipeline pipeline,
      Coder<EventKeyT> keyCoder,
      Coder<EventT> eventCoder,
      Coder<StateT> stateCoder,
      KvCoder<EventKeyT, ResultT> mainOutputCoder,
      KvCoder<EventKeyT, OrderedProcessingStatus> processingStatusCoder,
      KvCoder<EventKeyT, KV<Long, UnprocessedEvent<EventT>>> unprocessedEventsCoder) {
    PCollectionTuple processingResult;
    processingResult =
        input.apply(
            ParDo.of(
                    new SequencePerKeyProcessorDoFn<>(
                        handler.getEventExaminer(),
                        eventCoder,
                        stateCoder,
                        keyCoder,
                        mainOutput,
                        statusOutput,
                        handler.getStatusUpdateFrequency(),
                        unprocessedEventOutput,
                        handler.isProduceStatusUpdateOnEveryEvent(),
                        handler.getMaxOutputElementsPerBundle()))
                .withOutputTags(
                    mainOutput,
                    TupleTagList.of(Arrays.asList(statusOutput, unprocessedEventOutput))));
    return new OrderedEventProcessorResult<>(
        pipeline,
        processingResult.get(mainOutput).setCoder(mainOutputCoder),
        mainOutput,
        processingResult.get(statusOutput).setCoder(processingStatusCoder),
        statusOutput,
        processingResult.get(unprocessedEventOutput).setCoder(unprocessedEventsCoder),
        unprocessedEventOutput);
  }

  private OrderedEventProcessorResult<EventKeyT, ResultT, EventT> expandGlobalSequenceProcessing(
      PCollection<KV<EventKeyT, KV<Long, EventT>>> input,
      TupleTag<KV<EventKeyT, ResultT>> mainOutput,
      TupleTag<KV<EventKeyT, OrderedProcessingStatus>> statusOutput,
      TupleTag<KV<EventKeyT, KV<Long, UnprocessedEvent<EventT>>>> unprocessedEventOutput,
      OrderedProcessingHandler<EventT, EventKeyT, StateT, ResultT> handler,
      Pipeline pipeline,
      Coder<EventKeyT> keyCoder,
      Coder<EventT> eventCoder,
      Coder<StateT> stateCoder,
      KvCoder<EventKeyT, ResultT> mainOutputCoder,
      KvCoder<EventKeyT, OrderedProcessingStatus> processingStatusCoder,
      KvCoder<EventKeyT, KV<Long, UnprocessedEvent<EventT>>> unprocessedEventsCoder,
      OrderedProcessingGlobalSequenceHandler<EventT, EventKeyT, StateT, ResultT>
          globalSequenceHandler) {
    PCollectionTuple processingResult;
    boolean streamingProcessing = input.isBounded() == IsBounded.UNBOUNDED;

    final PCollectionView<ContiguousSequenceRange> latestContiguousRange =
        input
            .apply("Convert to SequenceAndTimestamp", ParDo.of(new ToTimestampedEventConverter<>()))
            .apply(
                "Global Sequence Tracker",
                streamingProcessing
                    ? new GlobalSequenceTracker<>(
                        globalSequenceHandler.getGlobalSequenceCombiner(),
                        globalSequenceHandler.getContiguousSequenceRangeReevaluationFrequency(),
                        globalSequenceHandler
                            .getMaxElementCountToTriggerContinuousSequenceRangeReevaluation())
                    : new GlobalSequenceTracker<>(
                        globalSequenceHandler.getGlobalSequenceCombiner()));

    if (streamingProcessing) {
      PCollection<KV<EventKeyT, KV<Long, EventT>>> tickers =
          input.apply(
              "Create Tickers",
              new PerKeyTickerGenerator<>(
                  keyCoder,
                  eventCoder,
                  globalSequenceHandler.getContiguousSequenceRangeReevaluationFrequency()));

      input =
          PCollectionList.of(input)
              .and(tickers)
              .apply("Combine Events and Tickers", Flatten.pCollections())
              .setCoder(tickers.getCoder());
    }
    processingResult =
        input.apply(
            ParDo.of(
                    new GlobalSequencesProcessorDoFn<>(
                        handler.getEventExaminer(),
                        eventCoder,
                        stateCoder,
                        keyCoder,
                        mainOutput,
                        statusOutput,
                        handler.getStatusUpdateFrequency(),
                        unprocessedEventOutput,
                        handler.isProduceStatusUpdateOnEveryEvent(),
                        handler.getMaxOutputElementsPerBundle(),
                        latestContiguousRange,
                        input.getWindowingStrategy().getAllowedLateness()))
                .withOutputTags(
                    mainOutput,
                    TupleTagList.of(Arrays.asList(statusOutput, unprocessedEventOutput)))
                .withSideInput(GLOBAL_SEQUENCE_TRACKER, latestContiguousRange));
    return new OrderedEventProcessorResult<>(
        pipeline,
        processingResult.get(mainOutput).setCoder(mainOutputCoder),
        mainOutput,
        processingResult.get(statusOutput).setCoder(processingStatusCoder),
        statusOutput,
        processingResult.get(unprocessedEventOutput).setCoder(unprocessedEventsCoder),
        unprocessedEventOutput,
        latestContiguousRange);
  }

  private static Coder<OrderedProcessingStatus> getOrderedProcessingStatusCoder(Pipeline pipeline) {
    SchemaRegistry schemaRegistry = pipeline.getSchemaRegistry();
    Coder<OrderedProcessingStatus> result;
    try {
      result =
          SchemaCoder.of(
              schemaRegistry.getSchema(OrderedProcessingStatus.class),
              TypeDescriptor.of(OrderedProcessingStatus.class),
              schemaRegistry.getToRowFunction(OrderedProcessingStatus.class),
              schemaRegistry.getFromRowFunction(OrderedProcessingStatus.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  static class ToTimestampedEventConverter<EventKeyT, EventT>
      extends DoFn<
          KV<EventKeyT, KV<Long, EventT>>, TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> {

    @ProcessElement
    public void convert(
        @Element KV<EventKeyT, KV<Long, EventT>> element,
        @Timestamp Instant timestamp,
        OutputReceiver<TimestampedValue<KV<EventKeyT, KV<Long, EventT>>>> outputReceiver) {
      outputReceiver.output(TimestampedValue.of(element, timestamp));
    }
  }
}
