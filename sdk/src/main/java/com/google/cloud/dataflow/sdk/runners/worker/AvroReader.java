/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A source that reads Avro files.
 *
 * @param <T> the type of the elements read from the source
 */
public class AvroReader<T> extends Reader<WindowedValue<T>> {
  private static final int BUF_SIZE = 200;
  final String filename;
  @Nullable
  final Long startPosition;
  @Nullable
  final Long endPosition;
  final AvroCoder<T> avroCoder;
  private final Schema schema;

  public AvroReader(String filename, @Nullable Long startPosition, @Nullable Long endPosition,
      WindowedValue.WindowedValueCoder<T> coder) {
    if (!(coder instanceof WindowedValue.ValueOnlyWindowedValueCoder)) {
      throw new IllegalArgumentException("Expected ValueOnlyWindowedValueCoder");
    }

    if (!(coder.getValueCoder() instanceof AvroCoder)) {
      throw new IllegalArgumentException("AvroReader requires an AvroCoder");
    }

    this.filename = filename;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.avroCoder = (AvroCoder<T>) coder.getValueCoder();
    this.schema = this.avroCoder.getSchema();
  }

  public ReaderIterator<WindowedValue<T>> iterator(DatumReader<T> datumReader) throws IOException {
    IOChannelFactory factory = IOChannelUtils.getFactory(filename);
    Collection<String> inputs = factory.match(filename);

    if (inputs.size() == 1) {
      String input = inputs.iterator().next();
      ReadableByteChannel reader = factory.open(input);
      return new AvroFileIterator(datumReader, input, reader, startPosition, endPosition);

    } else {
      if (startPosition != null || endPosition != null) {
        throw new UnsupportedOperationException(
            "Unable to apply range limits to multiple-input stream: " + filename);
      }
      return new AvroFileMultiIterator(datumReader, factory, inputs.iterator());
    }
  }

  @Override
  public ReaderIterator<WindowedValue<T>> iterator() throws IOException {
    return iterator(avroCoder.createDatumReader());
  }

  class AvroFileMultiIterator extends LazyMultiReaderIterator<WindowedValue<T>> {
    private final IOChannelFactory factory;
    private final DatumReader<T> datumReader;

    public AvroFileMultiIterator(
        DatumReader<T> datumReader, IOChannelFactory factory, Iterator<String> inputs) {
      super(inputs);
      this.factory = factory;
      this.datumReader = datumReader;
    }

    @Override
    protected ReaderIterator<WindowedValue<T>> open(String input) throws IOException {
      return new AvroFileIterator(datumReader, input, factory.open(input), null, null);
    }
  }

  class AvroFileIterator extends AbstractReaderIterator<WindowedValue<T>> {
    final DataFileReader<T> fileReader;
    final Long endOffset;

    public AvroFileIterator(DatumReader<T> datumReader, String filename, ReadableByteChannel reader,
        @Nullable Long startOffset, @Nullable Long endOffset) throws IOException {
      if (!(reader instanceof SeekableByteChannel)) {
        throw new UnsupportedOperationException(
            "Unable to seek to offset in stream for " + filename);
      }
      SeekableByteChannel inChannel = (SeekableByteChannel) reader;
      SeekableInput seekableInput = new SeekableByteChannelInput(inChannel);
      this.fileReader = new DataFileReader<>(seekableInput, datumReader);
      this.endOffset = endOffset;
      if (startOffset != null && startOffset > 0) {
        // Sync to the first record at or after startOffset.
        fileReader.sync(startOffset);
      }
    }

    @Override
    public boolean hasNext() throws IOException {
      return fileReader.hasNext() && (endOffset == null || !fileReader.pastSync(endOffset));
    }

    @Override
    public WindowedValue<T> next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T next = fileReader.next();
      // DataFileReader doesn't seem to support getting the current position.
      // The difference between tell() calls seems to be zero. Use the coder
      // instead.
      notifyElementRead(CoderUtils.encodeToByteArray(avroCoder, next).length);
      return WindowedValue.valueInGlobalWindow(next);
    }

    @Override
    public void close() throws IOException {
      fileReader.close();
    }
  }

  /**
   * An implementation of an Avro SeekableInput wrapping a
   * SeekableByteChannel.
   */
  static class SeekableByteChannelInput implements SeekableInput {
    final SeekableByteChannel channel;

    public SeekableByteChannelInput(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public void seek(long position) throws IOException {
      channel.position(position);
    }

    @Override
    public long tell() throws IOException {
      return channel.position();
    }

    @Override
    public long length() throws IOException {
      return channel.size();
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      return channel.read(ByteBuffer.wrap(b, offset, length));
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
