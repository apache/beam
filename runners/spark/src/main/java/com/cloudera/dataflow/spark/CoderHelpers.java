/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CoderHelpers {
  static byte[] toByteArray(Object value, Coder coder) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      coder.encode(value, baos, new Coder.Context(true));
    } catch (IOException e) {
      throw new RuntimeException("Error encoding value: " + value, e);
    }
    return baos.toByteArray();
  }

  static List<byte[]> toByteArrays(Iterable values, final Coder coder) {
    List<byte[]> res = Lists.newLinkedList();
    for (Object value : values) {
      res.add(toByteArray(value, coder));
    }
    return res;
  }

  static <T> T fromByteArray(byte[] serialized, Coder<T> coder) {
    ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
    try {
      return coder.decode(bais, new Coder.Context(true));
    } catch (IOException e) {
      throw new RuntimeException("Error decoding bytes for coder: " + coder, e);
    }
  }

  static <T> Function<T, byte[]> toByteFunction(final Coder<T> coder) {
    return new Function<T, byte[]>() {
      @Override
      public byte[] call(T t) throws Exception {
        return toByteArray(t, coder);
      }
    };
  }

  static <T> Function<byte[], T> fromByteFunction(final Coder<T> coder) {
    return new Function<byte[], T>() {
      @Override
      public T call(byte[] bytes) throws Exception {
        return fromByteArray(bytes, coder);
      }
    };
  }
}
