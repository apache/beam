package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

class GlobalSequencesProcessorDoFn<EventTypeT, EventKeyTypeT, ResultTypeT,
    StateTypeT extends MutableState<EventTypeT, ResultTypeT>>
    extends ProcessorDoFn<EventTypeT, EventKeyTypeT, ResultTypeT, StateTypeT> {

  /**
   * Stateful DoFn to do the bulk of processing.
   *
   * @param eventExaminer
   * @param eventCoder
   * @param stateCoder
   * @param keyCoder
   * @param mainOutputTupleTag
   * @param statusTupleTag
   * @param statusUpdateFrequency
   * @param unprocessedEventTupleTag
   * @param produceStatusUpdateOnEveryEvent
   * @param maxNumberOfResultsToProduce
   */
  GlobalSequencesProcessorDoFn(EventExaminer<EventTypeT, StateTypeT> eventExaminer,
      Coder<EventTypeT> eventCoder,
      Coder<StateTypeT> stateCoder,
      Coder<EventKeyTypeT> keyCoder,
      TupleTag<KV<EventKeyTypeT, ResultTypeT>> mainOutputTupleTag,
      TupleTag<KV<EventKeyTypeT, OrderedProcessingStatus>> statusTupleTag,
      Duration statusUpdateFrequency,
      TupleTag<KV<EventKeyTypeT, KV<Long, UnprocessedEvent<EventTypeT>>>>
          unprocessedEventTupleTag,
      boolean produceStatusUpdateOnEveryEvent, long maxNumberOfResultsToProduce) {
    super(eventExaminer, mainOutputTupleTag, statusTupleTag,
        statusUpdateFrequency, unprocessedEventTupleTag, produceStatusUpdateOnEveryEvent,
        maxNumberOfResultsToProduce);
  }
}
