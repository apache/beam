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
import org.apache.beam.sdk.coders.Coder;

/** Abstracts IO operations for different data types. */
// read/write individual records sequentially from a sorted bucket file
// Serializable so it can be shipped to DoFns
// @Todo: this goes in a PCollection, needs Coder, or is SerializableCoder OK?
public abstract class SortedBucketFile<ValueT> implements Serializable {

  public abstract Reader<ValueT> createReader();

  public abstract Writer<ValueT> createWriter();

  /**
   * Reader.
   *
   * @param <ValueT>
   */
  public abstract static class Reader<ValueT> implements Serializable {
    public abstract Coder<? extends Reader> coder();

    public abstract void prepareRead(ReadableByteChannel channel) throws Exception;

    public abstract ValueT read() throws Exception;

    public abstract void finishRead() throws Exception;
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
