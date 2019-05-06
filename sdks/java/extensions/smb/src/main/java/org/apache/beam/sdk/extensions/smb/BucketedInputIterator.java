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

import java.util.Arrays;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.smb.FileOperations.Reader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/** Iterates over shards in a bucket one record at a time. */
class BucketedInputIterator<K, V> {
  private final TupleTag tupleTag;
  private final Reader<V> reader;
  private final ResourceId resourceId;
  private final BucketMetadata<K, V> metadata;

  private KV<byte[], V> nextKv;

  BucketedInputIterator(
      Reader<V> reader,
      ResourceId resourceId,
      TupleTag<V> tupleTag,
      BucketMetadata<K, V> metadata) {
    this.reader = reader;
    this.metadata = metadata;
    this.resourceId = resourceId;
    this.tupleTag = tupleTag;

    initializeReader();
  }

  private void initializeReader() {
    V value;
    try {
      reader.prepareRead(FileSystems.open(resourceId));
      value = reader.read();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (value == null) {
      nextKv = null;
    } else {
      nextKv = KV.of(metadata.extractKeyBytes(value), value);
    }
  }

  TupleTag getTupleTag() {
    return tupleTag;
  }

  boolean hasNextKeyGroup() {
    return nextKv != null;
  }

  // group next continuous values of the same key in an iterator
  KV<byte[], Iterator<?>> nextKeyGroup() {
    final byte[] key = nextKv.getKey();

    Iterator<?> iterator =
        new Iterator<Object>() {
          V value = nextKv.getValue();

          @Override
          public boolean hasNext() {
            return value != null;
          }

          @Override
          public Object next() {
            try {
              final V result = value;
              final V v = reader.read();
              if (v == null) {
                // end of file, reset outer
                value = null;
                nextKv = null;
              } else {
                final byte[] k = metadata.extractKeyBytes(v);
                if (Arrays.equals(key, k)) {
                  // same key, update next value
                  value = v;
                } else {
                  // end of group, advance outer
                  value = null;
                  nextKv = KV.of(k, v);
                }
              }
              return result;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };

    return KV.of(key, iterator);
  }
}
