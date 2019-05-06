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
package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;

/** Abstracts IO operations for different data types with the granularity of a single record. */
public abstract class FileOperations<ValueT> implements Serializable {

  public Iterator<ValueT> iterator(ResourceId file) throws Exception {
    Reader<ValueT> reader = createReader();
    reader.prepareRead(FileSystems.open(file));
    return reader.iterator();
  }

  public abstract Reader<ValueT> createReader();

  public abstract Writer<ValueT> createWriter();

  /**
   * Reader for Collection type.
   *
   * @param <ValueT>
   */
  public abstract static class Reader<ValueT> implements Serializable {
    public abstract void prepareRead(ReadableByteChannel channel) throws Exception;

    /**
     * Reads next record in the collection. Should return null if EOF is reached. (@Todo: should we
     * have more clearly defined behavior for EOF?)
     */
    public abstract ValueT read() throws Exception;

    public abstract void finishRead() throws Exception;

    public Iterator<ValueT> iterator() throws Exception {
      return new Iterator<ValueT>() {
        private ValueT next = read();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public ValueT next() {
          if (next == null) {
            throw new NoSuchElementException();
          }
          ValueT result = next;
          try {
            next = read();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return result;
        }
      };
    }
  }

  /**
   * Writer.
   *
   * @param <ValueT>
   */
  public abstract static class Writer<ValueT> implements Serializable {
    public abstract String getMimeType();

    public abstract void prepareWrite(WritableByteChannel channel) throws Exception;

    public abstract void write(ValueT value) throws Exception;

    public abstract void finishWrite() throws Exception;
  }
}
