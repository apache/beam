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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimerInternals;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;

import javax.annotation.Nullable;

/**
 * An implementation of Beam's {@link TimerInternals}, that also provides serialization functionality.
 * The latter is used when snapshots of the current state are taken, for fault-tolerance.
 * */
public abstract class AbstractFlinkTimerInternals<K, VIN> implements TimerInternals, Serializable {
  private Instant currentInputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant currentOutputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public void setCurrentInputWatermark(Instant watermark) {
    checkIfValidInputWatermark(watermark);
    this.currentInputWatermark = watermark;
  }

  public void setCurrentOutputWatermark(Instant watermark) {
    checkIfValidOutputWatermark(watermark);
    this.currentOutputWatermark = watermark;
  }

  private void setCurrentInputWatermarkAfterRecovery(Instant watermark) {
    if (!currentInputWatermark.isEqual(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      throw new RuntimeException("Explicitly setting the input watermark is only allowed on " +
          "initialization after recovery from a node failure. Apparently this is not " +
          "the case here as the watermark is already set.");
    }
    this.currentInputWatermark = watermark;
  }

  private void setCurrentOutputWatermarkAfterRecovery(Instant watermark) {
    if (!currentOutputWatermark.isEqual(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      throw new RuntimeException("Explicitly setting the output watermark is only allowed on " +
        "initialization after recovery from a node failure. Apparently this is not " +
        "the case here as the watermark is already set.");
    }
    this.currentOutputWatermark = watermark;
  }

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return currentInputWatermark;
  }

  @Nullable
  @Override
  public Instant currentSynchronizedProcessingTime() {
    // TODO
    return null;
  }

  @Override
  public Instant currentOutputWatermarkTime() {
    return currentOutputWatermark;
  }

  private void checkIfValidInputWatermark(Instant newWatermark) {
    if (currentInputWatermark.isAfter(newWatermark)) {
      throw new IllegalArgumentException(String.format(
          "Cannot set current input watermark to %s. Newer watermarks " +
              "must be no earlier than the current one (%s).",
          newWatermark, currentInputWatermark));
    }
  }

  private void checkIfValidOutputWatermark(Instant newWatermark) {
    if (currentOutputWatermark.isAfter(newWatermark)) {
      throw new IllegalArgumentException(String.format(
        "Cannot set current output watermark to %s. Newer watermarks " +
          "must be no earlier than the current one (%s).",
        newWatermark, currentOutputWatermark));
    }
  }

  public void encodeTimerInternals(OldDoFn.ProcessContext context,
                                   StateCheckpointWriter writer,
                                   KvCoder<K, VIN> kvCoder,
                                   Coder<? extends BoundedWindow> windowCoder) throws IOException {
    if (context == null) {
      throw new RuntimeException("The Context has not been initialized.");
    }

    writer.setTimestamp(currentInputWatermark);
    writer.setTimestamp(currentOutputWatermark);
  }

  public void restoreTimerInternals(StateCheckpointReader reader,
                                    KvCoder<K, VIN> kvCoder,
                                    Coder<? extends BoundedWindow> windowCoder) throws IOException {
    setCurrentInputWatermarkAfterRecovery(reader.getTimestamp());
    setCurrentOutputWatermarkAfterRecovery(reader.getTimestamp());
  }
}
