/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A {@link UnboundedSource} created from {@link UnboundedDataSource}.
 */
public class BeamUnboundedSource<T, OFFSET extends Serializable>
    extends UnboundedSource<T, BeamUnboundedSource.BeamCheckpointMark<OFFSET>> {

  public static <T, OFFSET extends Serializable> BeamUnboundedSource<T, OFFSET> wrap(
      UnboundedDataSource<T, OFFSET> wrap) {
    return new BeamUnboundedSource<>(wrap);
  }

  public static class BeamCheckpointMark<OFFSET>
      implements UnboundedSource.CheckpointMark, Serializable {

    private final OFFSET offset;

    public BeamCheckpointMark(OFFSET off) {
      this.offset = off;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // nop
    }

  }

  private final UnboundedDataSource<T, OFFSET> wrap;
  private final int partitionId;

  private BeamUnboundedSource(UnboundedDataSource<T, OFFSET> wrap) {
    this(wrap, -1);
  }

  private BeamUnboundedSource(UnboundedDataSource<T, OFFSET> wrap, int partitionId) {
    this.wrap = Objects.requireNonNull(wrap);
    this.partitionId = partitionId;
  }

  @Override
  public void validate() {
    // FIXME
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return new KryoCoder<>();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BeamUnboundedSource) {
      BeamUnboundedSource ds = (BeamUnboundedSource) obj;
      return ds.wrap.equals(this.wrap) && ds.partitionId == partitionId;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(wrap, partitionId);
  }


  @Override
  public List<? extends UnboundedSource<T, BeamCheckpointMark<OFFSET>>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    if (partitionId == -1) {
      final List<BeamUnboundedSource<T, OFFSET>> splits;
      splits = new ArrayList<>(wrap.getPartitions().size());
      for (int i = 0; i < wrap.getPartitions().size(); i++) {
        splits.add(new BeamUnboundedSource<>(wrap, i));
      }
      return splits;
    } else {
      return Collections.singletonList(this);
    }
  }

  @Override
  public UnboundedReader<T> createReader(
      PipelineOptions options, BeamCheckpointMark<OFFSET> checkpointMark) throws IOException {

    final cz.seznam.euphoria.core.client.io.UnboundedReader<T, OFFSET> reader;
    reader = wrap.getPartitions().get(partitionId).openReader();
    return new UnboundedReader<T>() {

      private OFFSET offset = checkpointMark == null ? null : checkpointMark.offset;
      private T current = null;
      private boolean hasNext = false;
      {
        if (checkpointMark != null && checkpointMark.offset != null) {
          reader.reset(checkpointMark.offset);
        }
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        hasNext = reader.hasNext();
        if (hasNext) {
          current = reader.next();
        }
        return hasNext;
      }

      @Override
      public Instant getWatermark() {
        return hasNext ? new Instant(Long.MIN_VALUE) : new Instant(Long.MAX_VALUE);
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new BeamCheckpointMark<>(offset);
      }

      @Override
      public UnboundedSource<T, ?> getCurrentSource() {
        return BeamUnboundedSource.this;
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        offset = reader.getCurrentOffset();
        return current;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(Long.MIN_VALUE);
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

    };
  }

  @Override
  public Coder<BeamCheckpointMark<OFFSET>> getCheckpointMarkCoder() {
    return new KryoCoder<>();
  }

}
