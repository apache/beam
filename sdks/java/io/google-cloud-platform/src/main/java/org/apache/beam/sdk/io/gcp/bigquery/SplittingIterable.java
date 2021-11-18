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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1beta2.ProtoRows;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Takes in an iterable and batches the results into multiple ProtoRows objects. The splitSize
 * parameter controls how many rows are batched into a single ProtoRows object before we move on to
 * the next one.
 */
class SplittingIterable implements Iterable<ProtoRows> {
  private final Iterable<byte[]> underlying;
  private final long splitSize;

  public SplittingIterable(Iterable<byte[]> underlying, long splitSize) {
    this.underlying = underlying;
    this.splitSize = splitSize;
  }

  @Override
  public Iterator<ProtoRows> iterator() {
    return new Iterator<ProtoRows>() {
      final Iterator<byte[]> underlyingIterator = underlying.iterator();

      @Override
      public boolean hasNext() {
        return underlyingIterator.hasNext();
      }

      @Override
      public ProtoRows next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        ProtoRows.Builder inserts = ProtoRows.newBuilder();
        long bytesSize = 0;
        while (underlyingIterator.hasNext()) {
          ByteString byteString = ByteString.copyFrom(underlyingIterator.next());
          inserts.addSerializedRows(byteString);
          bytesSize += byteString.size();
          if (bytesSize > splitSize) {
            break;
          }
        }
        return inserts.build();
      }
    };
  }
}
