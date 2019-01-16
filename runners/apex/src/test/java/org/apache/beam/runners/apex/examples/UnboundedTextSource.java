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
package org.apache.beam.runners.apex.examples;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/** unbounded source that reads from text. */
public class UnboundedTextSource extends UnboundedSource<String, UnboundedSource.CheckpointMark> {
  private static final long serialVersionUID = 1L;

  @Override
  public List<? extends UnboundedSource<String, CheckpointMark>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    return Collections.<UnboundedSource<String, CheckpointMark>>singletonList(this);
  }

  @Override
  public UnboundedReader<String> createReader(
      PipelineOptions options, @Nullable CheckpointMark checkpointMark) {
    return new UnboundedTextReader(this);
  }

  @Nullable
  @Override
  public Coder<CheckpointMark> getCheckpointMarkCoder() {
    return null;
  }

  @Override
  public Coder<String> getOutputCoder() {
    return StringUtf8Coder.of();
  }

  /** reads from text. */
  public static class UnboundedTextReader extends UnboundedReader<String> implements Serializable {

    private static final long serialVersionUID = 7526472295622776147L;

    private final UnboundedTextSource source;

    private final String[] texts = new String[] {"foo foo foo bar bar", "foo foo bar bar bar"};
    private long index = 0;

    private String currentRecord;

    private Instant currentTimestamp;

    public UnboundedTextReader(UnboundedTextSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      currentRecord = texts[0];
      currentTimestamp = new Instant(0);
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      index++;
      currentRecord = texts[(int) index % (texts.length)];
      currentTimestamp = new Instant(index * 1000);
      try {
        Thread.sleep(index); // allow for downstream processing to complete
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    }

    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      return new byte[0];
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return this.currentRecord;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return currentTimestamp;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Instant getWatermark() {
      return currentTimestamp;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return null;
    }

    @Override
    public UnboundedSource<String, ?> getCurrentSource() {
      return this.source;
    }
  }
}
