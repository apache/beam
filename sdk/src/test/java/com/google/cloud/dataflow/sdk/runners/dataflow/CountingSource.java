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

package com.google.cloud.dataflow.sdk.runners.dataflow;

import static com.google.cloud.dataflow.sdk.util.CoderUtils.encodeToByteArray;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

/**
 * An unbounded source for testing the unbounded sources framework code.
 *
 * <p> Each split of this sources produces records of the form KV(split_id, i),
 * where i counts up from 0.  Each record has a timestamp of i, and the watermark
 * accurately tracks these timestamps.  The reader will occasionally return false
 * from {@code advance}, in order to simulate a source where not all the data is
 * available immediately.
 */
public class CountingSource
    extends UnboundedSource<KV<Integer, Integer>, CountingSource.CounterMark> {
  private static final long serialVersionUID = 0L;
  private static List<Integer> finalizeTracker;
  private final int numMessagesPerShard;
  private final int shardNumber;
  private final boolean dedup;

  public static void setFinalizeTracker(List<Integer> finalizeTracker) {
    CountingSource.finalizeTracker = finalizeTracker;
  }

  public CountingSource(int numMessagesPerShard) {
    this(numMessagesPerShard, 0, false);
  }

  public CountingSource withDedup() {
    return new CountingSource(numMessagesPerShard, shardNumber, true);
  }

  private CountingSource withShardNumber(int shardNumber) {
    return new CountingSource(numMessagesPerShard, shardNumber, dedup);
  }

  private CountingSource(int numMessagesPerShard, int shardNumber, boolean dedup) {
    this.numMessagesPerShard = numMessagesPerShard;
    this.shardNumber = shardNumber;
    this.dedup = dedup;
  }

  public int getShardNumber() {
    return shardNumber;
  }

  @Override
  public List<CountingSource> generateInitialSplits(int desiredNumSplits, PipelineOptions options) {
    List<CountingSource> splits = new ArrayList<>();
    for (int i = 0; i < desiredNumSplits; i++) {
      splits.add(withShardNumber(i));
    }
    return splits;
  }

  class CounterMark implements UnboundedSource.CheckpointMark {
    int current;

    public CounterMark(int current) {
      this.current = current;
    }

    @Override
    public void finalizeCheckpoint() {
      if (finalizeTracker != null) {
        finalizeTracker.add(current);
      }
    }
  }

  @Override
  public Coder<CounterMark> getCheckpointMarkCoder() {
    return DelegateCoder.of(
        VarIntCoder.of(),
        new DelegateCoder.CodingFunction<CounterMark, Integer>() {
          private static final long serialVersionUID = 0L;

          @Override
          public Integer apply(CounterMark input) {
            return input.current;
          }
        },
        new DelegateCoder.CodingFunction<Integer, CounterMark>() {
          private static final long serialVersionUID = 0L;

          @Override
          public CounterMark apply(Integer input) {
            return new CounterMark(input);
          }
        });
  }

  @Override
  public boolean requiresDeduping() {
    return dedup;
  }

  private class CountingSourceReader extends UnboundedReader<KV<Integer, Integer>> {
    private int current;
    private boolean done = false;

    public CountingSourceReader(int startingPoint) {
      this.current = startingPoint;
    }

    @Override
    public boolean start() {
      return true;
    }

    @Override
    public boolean advance() {
      if (current < numMessagesPerShard - 1) {
        // If testing dedup, occasionally insert a duplicate value;
        if (dedup && ThreadLocalRandom.current().nextInt(5) == 0) {
          return true;
        }
        current++;
        return true;
      } else {
        done = true;
        return false;
      }
    }

    @Override
    public KV<Integer, Integer> getCurrent() {
      return KV.of(shardNumber, current);
    }

    @Override
    public Instant getCurrentTimestamp() {
      return new Instant(current);
    }

    @Override
    public byte[] getCurrentRecordId() {
      try {
        return encodeToByteArray(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()), getCurrent());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {}

    @Override
    public CountingSource getCurrentSource() {
      return CountingSource.this;
    }

    @Override
    public Instant getWatermark() {
      return done ? BoundedWindow.TIMESTAMP_MAX_VALUE : new Instant(current - 1);
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new CounterMark(current);
    }
  }

  @Override
  public CountingSourceReader createReader(
      PipelineOptions options, @Nullable CounterMark checkpointMark) {
    return new CountingSourceReader(checkpointMark != null ? checkpointMark.current : 0);
  }

  @Override
  public void validate() {}

  @Override
  public Coder<KV<Integer, Integer>> getDefaultOutputCoder() {
    return KvCoder.of(VarIntCoder.of(), VarIntCoder.of());
  }
}
