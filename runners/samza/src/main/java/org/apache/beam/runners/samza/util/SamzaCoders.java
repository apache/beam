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

package org.apache.beam.runners.samza.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.samza.serializers.Serde;

/**
 * Utils for Coders creation/conversion in Samza.
 */
public class SamzaCoders {

  private SamzaCoders() {}

  public static <T> Coder<WindowedValue<T>> of(PCollection<T> pCollection) {
    final Coder<T> coder = pCollection.getCoder();
    final Coder<? extends BoundedWindow> windowCoder = pCollection.getWindowingStrategy()
        .getWindowFn().windowCoder();
    return WindowedValue.FullWindowedValueCoder.of(coder, windowCoder);
  }

  public static <T> Serde<T> toSerde(final Coder<T> coder) {
    return new Serde<T>() {
      @Override
      public T fromBytes(byte[] bytes) {
        if (bytes != null) {
          final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          try {
            return (T) coder.decode(bais);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else {
          return null;
        }
      }

      @Override
      public byte[] toBytes(T t) {
        if (t != null) {
          final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            coder.encode(t, baos);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return baos.toByteArray();
        } else {
          return null;
        }
      }
    };
  }

}
