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
package org.apache.beam.runners.flink.streaming;

import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
<<<<<<<< HEAD:runners/flink/1.11/src/test/java/org/apache/beam/runners/flink/streaming/StreamSources.java
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
========
>>>>>>>> master:runners/flink/1.14/src/test/java/org/apache/beam/runners/flink/streaming/StreamSources.java
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.RegularOperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/** {@link StreamSource} utilities, that bridge incompatibilities between Flink releases. */
public class StreamSources {

  public static <OutT, SrcT extends SourceFunction<OutT>> void run(
      StreamSource<OutT, SrcT> streamSource,
      Object lockingObject,
      Output<StreamRecord<OutT>> collector)
      throws Exception {
<<<<<<<< HEAD:runners/flink/1.11/src/test/java/org/apache/beam/runners/flink/streaming/StreamSources.java
    streamSource.run(
        lockingObject,
        new TestStreamStatusMaintainer(),
        collector,
        createOperatorChain(streamSource));
========
    streamSource.run(lockingObject, collector, createOperatorChain(streamSource));
>>>>>>>> master:runners/flink/1.14/src/test/java/org/apache/beam/runners/flink/streaming/StreamSources.java
  }

  private static OperatorChain<?, ?> createOperatorChain(AbstractStreamOperator<?> operator) {
    return new RegularOperatorChain<>(
        operator.getContainingTask(),
        StreamTask.createRecordWriterDelegate(
            operator.getOperatorConfig(), new MockEnvironmentBuilder().build()));
  }

<<<<<<<< HEAD:runners/flink/1.11/src/test/java/org/apache/beam/runners/flink/streaming/StreamSources.java
  /** StreamStatusMaintainer was removed in Flink 1.14. */
  private static final class TestStreamStatusMaintainer implements StreamStatusMaintainer {
    StreamStatus currentStreamStatus = StreamStatus.ACTIVE;

    @Override
    public void toggleStreamStatus(StreamStatus streamStatus) {
      if (!currentStreamStatus.equals(streamStatus)) {
        currentStreamStatus = streamStatus;
      }
    }

    @Override
    public StreamStatus getStreamStatus() {
      return currentStreamStatus;
    }
  }

  /** The emitWatermarkStatus method was added in Flink 1.14, so we need to wrap Output. */
  public interface OutputWrapper<T> extends Output<T> {}
========
  /** The emitWatermarkStatus method was added in Flink 1.14, so we need to wrap Output. */
  public interface OutputWrapper<T> extends Output<T> {
    @Override
    default void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}
  }
>>>>>>>> master:runners/flink/1.14/src/test/java/org/apache/beam/runners/flink/streaming/StreamSources.java
}
