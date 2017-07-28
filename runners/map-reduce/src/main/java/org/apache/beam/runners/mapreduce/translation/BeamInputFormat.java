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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Adaptor from Beam {@link BoundedSource} to MapReduce {@link InputFormat}.
 */
public class BeamInputFormat<T> extends InputFormat {

  public static final String BEAM_SERIALIZED_BOUNDED_SOURCE = "beam-serialized-bounded-source";
  private static final long DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES = 5 * 1000 * 1000;

  private BoundedSource<T> source;
  private PipelineOptions options;

  public BeamInputFormat() {
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    String serializedBoundedSource = context.getConfiguration().get(BEAM_SERIALIZED_BOUNDED_SOURCE);
    if (Strings.isNullOrEmpty(serializedBoundedSource)) {
      return ImmutableList.of();
    }
    source = (BoundedSource<T>) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedBoundedSource), "BoundedSource");
    try {
      return FluentIterable.from(source.split(DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES, options))
          .transform(new Function<BoundedSource<T>, InputSplit>() {
            @Override
            public InputSplit apply(BoundedSource<T> source) {
              try {
                return new BeamInputSplit(source.getEstimatedSizeBytes(options));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }})
          .toList();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // TODO: it should initiates from InputSplit.
    source = (BoundedSource<T>) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(context.getConfiguration().get(BEAM_SERIALIZED_BOUNDED_SOURCE)),
        "");
    return new BeamRecordReader<>(source.createReader(options));
  }

  public static class BeamInputSplit extends InputSplit implements Writable {
    private long estimatedSizeBytes;

    public BeamInputSplit() {
    }

    BeamInputSplit(long estimatedSizeBytes) {
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return estimatedSizeBytes;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(estimatedSizeBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      estimatedSizeBytes = in.readLong();
    }
  }

  private class BeamRecordReader<T> extends RecordReader {

    private final BoundedSource.BoundedReader<T> reader;
    private boolean started;

    public BeamRecordReader(BoundedSource.BoundedReader<T> reader) {
      this.reader = checkNotNull(reader, "reader");
      this.started = false;
    }

    @Override
    public void initialize(
        InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return reader.advance();
      }
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return "global";
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return WindowedValue.timestampedValueInGlobalWindow(
          reader.getCurrent(), reader.getCurrentTimestamp());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      Double progress = reader.getFractionConsumed();
      if (progress != null) {
        return progress.floatValue();
      } else {
        return 0;
      }
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
