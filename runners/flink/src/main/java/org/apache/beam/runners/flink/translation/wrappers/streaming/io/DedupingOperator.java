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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.adapter.FlinkKey;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.joda.time.Duration;

/** Remove values with duplicate ids. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DedupingOperator<T> extends AbstractStreamOperator<WindowedValue<T>>
    implements OneInputStreamOperator<WindowedValue<ValueWithRecordId<T>>, WindowedValue<T>>,
        Triggerable<FlinkKey, VoidNamespace> {

  private static final long MAX_RETENTION_SINCE_ACCESS = Duration.standardMinutes(10L).getMillis();
  private final SerializablePipelineOptions options;

  // we keep the time when we last saw an element id for cleanup
  private ValueStateDescriptor<Long> dedupingStateDescriptor =
      new ValueStateDescriptor<>("dedup-cache", LongSerializer.INSTANCE);

  private transient InternalTimerService<VoidNamespace> timerService;

  public DedupingOperator(PipelineOptions options) {
    this.options = new SerializablePipelineOptions(options);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    timerService =
        getInternalTimerService("dedup-cleanup-timer", VoidNamespaceSerializer.INSTANCE, this);
  }

  @Override
  public void open() {
    // Initialize FileSystems for any coders which may want to use the FileSystem,
    // see https://issues.apache.org/jira/browse/BEAM-8303
    FileSystems.setDefaultPipelineOptions(options.get());
  }

  @Override
  public void processElement(StreamRecord<WindowedValue<ValueWithRecordId<T>>> streamRecord)
      throws Exception {

    ValueState<Long> dedupingState = getPartitionedState(dedupingStateDescriptor);

    Long lastSeenTimestamp = dedupingState.value();

    if (lastSeenTimestamp == null) {
      // we have never seen this, emit
      WindowedValue<ValueWithRecordId<T>> value = streamRecord.getValue();
      output.collect(streamRecord.replace(value.withValue(value.getValue().getValue())));
    }

    long currentProcessingTime = timerService.currentProcessingTime();
    dedupingState.update(currentProcessingTime);
    timerService.registerProcessingTimeTimer(
        VoidNamespace.INSTANCE, currentProcessingTime + MAX_RETENTION_SINCE_ACCESS);
  }

  @Override
  public void onEventTime(InternalTimer<FlinkKey, VoidNamespace> internalTimer) {
    // will never happen
  }

  @Override
  public void onProcessingTime(InternalTimer<FlinkKey, VoidNamespace> internalTimer)
      throws Exception {
    ValueState<Long> dedupingState = getPartitionedState(dedupingStateDescriptor);

    Long lastSeenTimestamp = dedupingState.value();
    if (lastSeenTimestamp != null
        && lastSeenTimestamp.equals(internalTimer.getTimestamp() - MAX_RETENTION_SINCE_ACCESS)) {
      dedupingState.clear();
    }
  }
}
