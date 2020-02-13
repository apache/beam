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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.io.Closeable;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

/** A write operation. */
public class WriteOperation extends ReceivingOperation {
  /** The Sink this operation writes to. */
  public final Sink<?> sink;

  /** Invalid sink index for reportSinkIndexIfEmptyOutput(). */
  protected static final int INVALID_SINK_INDEX = -1;

  /** The total byte counter for all data written by this operation. */
  final Counter<Long, Long> byteCount;

  /** The Sink's writer this operation writes to, created by start(). */
  Sink.SinkWriter<Object> writer;

  protected WriteOperation(
      Sink<?> sink,
      OutputReceiver[] receivers,
      OperationContext context,
      CounterName bytesCounterName) {
    super(receivers, context);
    this.sink = sink;
    this.byteCount = context.counterFactory().longSum(bytesCounterName);
  }

  private static CounterName bytesCounterName(OperationContext context) {
    return CounterName.named(context.nameContext().systemName() + "-ByteCount");
  }

  public static WriteOperation create(
      Sink<?> sink, OutputReceiver[] receivers, OperationContext context) {
    return new WriteOperation(sink, receivers, context, bytesCounterName(context));
  }

  public static WriteOperation forTest(Sink<?> sink, OperationContext context) {
    return create(sink, new OutputReceiver[] {}, context);
  }

  public Sink<?> getSink() {
    return sink;
  }

  @SuppressWarnings("unchecked")
  protected void initializeWriter() throws Exception {
    writer = (Sink.SinkWriter<Object>) sink.writer();
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();
      initializeWriter();
    }
  }

  protected void mayInitializeWriterInProcess() throws Exception {}

  @Override
  public void process(Object outputElem) throws Exception {
    try (Closeable scope = context.enterProcess()) {
      checkStarted();
      mayInitializeWriterInProcess();
      byteCount.addValue(writer.add(outputElem));
    }
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      checkStarted();
      try {
        if (writer != null) {
          writer.close();
        }
      } finally {
        super.finish();
        writer = null;
      }
    }
  }

  @Override
  public void abort() throws Exception {
    if (writer == null) {
      // If we haven't started, or have already closed the writer, we cannot abort it.
      super.abort();
      return;
    }

    try (Closeable scope = context.enterAbort()) {
      writer.abort();
    } finally {
      super.abort();
      writer = null;
    }
  }

  @Override
  public boolean supportsRestart() {
    return sink.supportsRestart();
  }

  public Counter<Long, Long> getByteCount() {
    return byteCount;
  }

  /**
   * Reports the sink index of a WriteOperation if the WriteOperation did not produce any output.
   * Returns INVALID_SINK_INDEX for a WriteOperation that produced output. NOTE: this is only used
   * by FlumeWriteOperation. TODO: Report system_name instead of sink_index when enabling this
   * feature on Dataflow.
   */
  public int reportSinkIndexIfEmptyOutput() {
    return INVALID_SINK_INDEX;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("sink", sink)
        .add("writer", writer)
        .add("byteCount", byteCount)
        .toString();
  }
}
