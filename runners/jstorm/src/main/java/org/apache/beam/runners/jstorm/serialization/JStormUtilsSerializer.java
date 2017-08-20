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

import backtype.storm.Config;
import com.alibaba.jstorm.cache.ComposedKey;
import com.alibaba.jstorm.cache.KvStoreIterable;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Specific serializer of {@link Kryo} for the Utils of JStorm Runner.
 */
public class JStormUtilsSerializer {

  /**
   * Specific {@link Kryo} serializer for {@link ComposedKey}.
   */
  public static class ComposedKeySerializer extends Serializer<ComposedKey> {
    public ComposedKeySerializer() {
      setImmutable(true);
    }

    @Override
    public ComposedKey read(final Kryo kryo, final Input input, final Class<ComposedKey> type) {
      final ComposedKey ret = ComposedKey.of();
      int len = input.readInt(true);
      for (int i = 0; i < len; i++) {
        Object obj = kryo.readClassAndObject(input);
        ret.add(obj);
      }
      return ret;
    }

    @Override
    public void write(final Kryo kryo, final Output output, final ComposedKey object) {
      int len = object.size();
      output.writeInt(len, true);
      for (Object elem : object) {
        kryo.writeClassAndObject(output, elem);
      }
    }
  }

  private static void registerComposedKeySerializers(Config config) {
    config.registerSerialization(ComposedKey.class, ComposedKeySerializer.class);
  }

  /**
   * Specific serializer of {@link Kryo} for KvStoreIterable.
   */
  public static class KvStoreIterableSerializer extends Serializer<KvStoreIterable<Object>> {

    public KvStoreIterableSerializer() {

    }

    @Override
    public void write(Kryo kryo, Output output, KvStoreIterable<Object> object) {
      int len = Iterables.size(object);
      output.writeInt(len, true);
      Iterator<Object> itr = object.iterator();
      while (itr.hasNext()) {
        Object elem = itr.next();
        kryo.writeClassAndObject(output, elem);
      }
    }

    @Override
    public KvStoreIterable<Object> read(Kryo kryo, Input input,
                                        Class<KvStoreIterable<Object>> type) {
      final int size = input.readInt(true);
      List<Object> values = Lists.newArrayList();
      for (int i = 0; i < size; ++i) {
        Object elem = kryo.readClassAndObject(input);
        values.add(elem);
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

  public static void registerSerializers(Config config) {
    registerComposedKeySerializers(config);
    config.registerDefaultSerailizer(KvStoreIterable.class, KvStoreIterableSerializer.class);
  }
}
