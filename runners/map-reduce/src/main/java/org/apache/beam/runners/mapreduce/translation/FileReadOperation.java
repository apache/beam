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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * Operation that reads from files.
 */
public class FileReadOperation<T> extends SourceOperation<WindowedValue<T>> {

  public FileReadOperation(int producerStageId, String fileName, Coder<T> coder) {
    super(new FileBoundedSource<>(producerStageId, fileName, coder));
  }

  private static class FileBoundedSource<T> extends BoundedSource<WindowedValue<T>> {

    private final int producerStageId;
    private final String fileName;
    private final Coder<WindowedValue<T>> coder;

    FileBoundedSource(int producerStageId, String fileName, Coder<T> coder) {
      this.producerStageId = producerStageId;
      this.fileName = checkNotNull(fileName, "fileName");
      checkNotNull(coder, "coder");
      this.coder = WindowedValue.getFullCoder(
          coder, WindowingStrategy.globalDefault().getWindowFn().windowCoder());

    }

    @Override
    public List<? extends BoundedSource<WindowedValue<T>>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // TODO: support split.
      return ImmutableList.of(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<WindowedValue<T>> createReader(PipelineOptions options)
        throws IOException {
      Path pattern = new Path(String.format("/tmp/mapreduce/stage-2/%s*", fileName));
      // TODO: use config from the job.
      Configuration conf = new Configuration();
      conf.set(
          "io.serializations",
          "org.apache.hadoop.io.serializer.WritableSerialization,"
              + "org.apache.hadoop.io.serializer.JavaSerialization");
      FileSystem fs = pattern.getFileSystem(conf);
      FileStatus[] files = fs.globStatus(pattern);
      Queue<SequenceFile.Reader> readers = new LinkedList<>();
      for (FileStatus f : files) {
        readers.add(new SequenceFile.Reader(fs, files[0].getPath(), conf));
      }
      return new Reader<>(this, readers, coder);
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<WindowedValue<T>> getDefaultOutputCoder() {
      return coder;
    }

    private static class Reader<T> extends BoundedReader<WindowedValue<T>> {

      private final BoundedSource<WindowedValue<T>> boundedSource;
      private final Queue<SequenceFile.Reader> readers;
      private final Coder<WindowedValue<T>> coder;
      private final BytesWritable value = new BytesWritable();

      Reader(
          BoundedSource<WindowedValue<T>> boundedSource,
          Queue<SequenceFile.Reader> readers,
          Coder<WindowedValue<T>> coder) {
        this.boundedSource = checkNotNull(boundedSource, "boundedSource");
        this.readers = checkNotNull(readers, "readers");
        this.coder = checkNotNull(coder, "coder");
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        SequenceFile.Reader reader = readers.peek();
        if (reader == null) {
          return false;
        }
        boolean hasNext = reader.next(NullWritable.get(), value);
        if (hasNext) {
          return true;
        } else {
          reader.close();
          readers.remove(reader);
          return advance();
        }
      }

      @Override
      public WindowedValue<T> getCurrent() throws NoSuchElementException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(value.getBytes());
        try {
          return coder.decode(inStream);
        } catch (IOException e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public BoundedSource<WindowedValue<T>> getCurrentSource() {
        return boundedSource;
      }
    }
  }
}
