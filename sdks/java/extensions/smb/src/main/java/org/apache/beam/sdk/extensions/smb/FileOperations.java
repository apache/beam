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

/** Abstracts IO operations sorted-bucket files. */
public abstract class FileOperations<V> implements Serializable {

  public abstract Reader<V> createReader();

  public abstract Writer<V> createWriter();

  public Iterator<V> iterator(ResourceId file) throws Exception {
    Reader<V> reader = createReader();
    reader.prepareRead(FileSystems.open(file));
    return reader.iterator();
  }

  /** Sorted-bucket file reader. */
  public abstract static class Reader<V> implements Serializable {
    public abstract void prepareRead(ReadableByteChannel channel) throws Exception;

    /**
     * Reads next record in the collection. Should return null if EOF is reached. (@Todo: should we
     * have more clearly defined behavior for EOF?)
     */
    public abstract V read() throws Exception;

    public abstract void finishRead() throws Exception;

    Iterator<V> iterator() throws Exception {
      return new Iterator<V>() {
        private V next = read();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public V next() {
          if (next == null) {
            throw new NoSuchElementException();
          }
          V result = next;
          try {
            next = read();
            if (next == null) {
              finishRead();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return result;
        }
      };
    }
  }

  /** Sorted-bucket file writer. */
  public abstract static class Writer<V> implements Serializable, AutoCloseable {
    public abstract String getMimeType();

    public abstract void prepareWrite(WritableByteChannel channel) throws Exception;

    public abstract void write(V value) throws Exception;

    @Override
    public abstract void close() throws Exception;
  }
}
