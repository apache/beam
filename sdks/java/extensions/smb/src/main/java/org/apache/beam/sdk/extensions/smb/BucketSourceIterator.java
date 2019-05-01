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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Reader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Iterates over shards in a bucket one record at a time.
 *
 * @param <KeyT>
 */
class BucketSourceIterator<KeyT> implements Serializable {
  private final TupleTag tupleTag;
  private final Reader<?> reader;
  private final ResourceId resourceId;
  private final BucketMetadata<KeyT, Object> metadata;

  private KV<KeyT, ?> nextKv;

  BucketSourceIterator(
      Reader<?> reader,
      ResourceId resourceId,
      TupleTag<?> tupleTag,
      BucketMetadata<KeyT, Object> metadata) {
    this.reader = reader;
    this.metadata = metadata;
    this.resourceId = resourceId;
    this.tupleTag = tupleTag;
  }

  void initializeReader() {
    Object value;
    try {
      reader.prepareRead(FileSystems.open(resourceId));
      value = reader.read();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (value == null) {
      nextKv = null;
    } else {
      nextKv = KV.of(metadata.extractSortingKey(value), value);
    }
  }

  TupleTag getTupleTag() {
    return tupleTag;
  }

  boolean hasNextKeyGroup() {
    return nextKv != null;
  }

  // group next continuous values of the same key in an iterator
  KV<KeyT, Iterator<?>> nextKeyGroup() {
    KeyT key = nextKv.getKey();

    Iterator<?> iterator =
        new Iterator<Object>() {
          Object value = nextKv.getValue();

          @Override
          public boolean hasNext() {
            return value != null;
          }

          @Override
          public Object next() {
            try {
              Object result = value;
              Object v = reader.read();
              if (v == null) {
                // end of file, reset outer
                value = null;
                nextKv = null;
              } else {
                KeyT k = metadata.extractSortingKey(v);
                if (key.equals(k)) {
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

  static class BucketSourceIteratorCoder<K> extends AtomicCoder<BucketSourceIterator<K>> {
    private Map<String, Coder<Reader>> readerCoderRegistry;

    BucketSourceIteratorCoder(Map<String, Coder<Reader>> readerCoderRegistry) {
      this.readerCoderRegistry = readerCoderRegistry;
    }

    @Override
    public void encode(BucketSourceIterator<K> value, OutputStream outStream)
        throws CoderException, IOException {
      try {
        ResourceIdCoder.of().encode(value.resourceId, outStream);
        SerializableCoder.of(TupleTag.class).encode(value.tupleTag, outStream);
        readerCoderRegistry.get(value.tupleTag.getId()).encode(value.reader, outStream);
        BucketMetadata.to(value.metadata, outStream);
      } catch (Exception e) {
        throw new CoderException("Encoding BucketSourceReader failed: " + e);
      }
    }

    @Override
    public BucketSourceIterator<K> decode(InputStream inStream) throws CoderException, IOException {
      try {
        final ResourceId resourceId = ResourceIdCoder.of().decode(inStream);
        final TupleTag<?> tupleTag = SerializableCoder.of(TupleTag.class).decode(inStream);
        final Reader<?> reader = readerCoderRegistry.get(tupleTag.getId()).decode(inStream);
        final BucketMetadata<K, Object> metadata =
            BucketMetadata.from(new ByteArrayInputStream(ByteArrayCoder.of().decode(inStream)));

        return new BucketSourceIterator<>(reader, resourceId, tupleTag, metadata);
      } catch (Exception e) {
        throw new CoderException("Decoding BucketSourceReader failed: " + e);
      }
    }
  }
}
