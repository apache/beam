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
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A source that reads Avro files. Records are read from the Avro file as a
 * series of byte arrays. The coder provided is used to deserialize each record
 * from a byte array.
 *
 * @param <T> the type of the elements read from the source
 */
public class AvroByteReader<T> extends Reader<T> {
  final AvroReader<ByteBuffer> avroReader;
  final Coder<T> coder;
  private final Schema schema = Schema.create(Schema.Type.BYTES);

  public AvroByteReader(
      String filename, @Nullable Long startPosition, @Nullable Long endPosition, Coder<T> coder) {
    this.coder = coder;
    avroReader = new AvroReader<>(filename, startPosition, endPosition,
        WindowedValue.getValueOnlyCoder(AvroCoder.of(ByteBuffer.class, schema)));
  }

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    return new AvroByteFileIterator();
  }

  class AvroByteFileIterator extends AbstractReaderIterator<T> {
    private final ReaderIterator<WindowedValue<ByteBuffer>> avroFileIterator;

    public AvroByteFileIterator() throws IOException {
      avroFileIterator = avroReader.iterator(new GenericDatumReader<ByteBuffer>(schema));
    }

    @Override
    public boolean hasNext() throws IOException {
      return avroFileIterator.hasNext();
    }

    @Override
    public T next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ByteBuffer inBuffer = avroFileIterator.next().getValue();
      byte[] encodedElem = new byte[inBuffer.remaining()];
      inBuffer.get(encodedElem);
      assert inBuffer.remaining() == 0;
      inBuffer.clear();
      notifyElementRead(encodedElem.length);
      return CoderUtils.decodeFromByteArray(coder, encodedElem);
    }

    @Override
    public void close() throws IOException {
      avroFileIterator.close();
    }
  }
}
