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
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;

import java.util.Collections;
import java.util.List;


/**
 * Specific serializer of {@link Kryo} for Collections.
 */
public class CollectionsSerializer {

  /**
   * Specific {@link Kryo} serializer for {@link java.util.Collections.SingletonList}.
   */
  public static class CollectionsSingletonListSerializer extends Serializer<List<?>> {
    public CollectionsSingletonListSerializer() {
      setImmutable(true);
    }

    @Override
    public List<?> read(final Kryo kryo, final Input input, final Class<List<?>> type) {
      final Object obj = kryo.readClassAndObject(input);
      return Collections.singletonList(obj);
    }

    @Override
    public void write(final Kryo kryo, final Output output, final List<?> list) {
      kryo.writeClassAndObject(output, list.get(0));
    }

  }

  public static void registerSerializers(Config config) {
    config.registerSerialization(Collections.singletonList("").getClass(),
            CollectionsSingletonListSerializer.class);
  }
}
