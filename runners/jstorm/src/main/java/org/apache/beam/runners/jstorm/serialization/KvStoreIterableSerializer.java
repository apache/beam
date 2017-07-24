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
package org.apache.beam.runners.jstorm.serialization;

import com.alibaba.jstorm.cache.KvStoreIterable;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;

/**
 * Specific serializer of {@link Kryo} for KvStoreIterable.
 */
public class KvStoreIterableSerializer extends Serializer<KvStoreIterable<Object>> {

  public KvStoreIterableSerializer() {

  }

  @Override
  public void write(Kryo kryo, Output output, KvStoreIterable<Object> object) {
    List<Object> values = Lists.newArrayList(object);
    output.writeInt(values.size(), true);
    for (Object elm : object) {
      kryo.writeClassAndObject(output, elm);
    }
  }

  @Override
  public KvStoreIterable<Object> read(Kryo kryo, Input input, Class<KvStoreIterable<Object>> type) {
    final int size = input.readInt(true);
    List<Object> values = Lists.newArrayList();
    for (int i = 0; i < size; ++i) {
      values.add(kryo.readClassAndObject(input));
    }

    return new KvStoreIterable<Object>() {
      Iterable<Object> values;

      @Override
      public Iterator<Object> iterator() {
        return values.iterator();
      }

      public KvStoreIterable init(Iterable<Object> values) {
        this.values = values;
        return this;
      }

      @Override
      public String toString() {
        return values.toString();
      }
    }.init(values);
  }
}
