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

package org.apache.beam.runners.spark.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broadcast helper.
 */
public abstract class BroadcastHelper<T> implements Serializable {

  /**
   * If the property {@code beam.spark.directBroadcast} is set to
   * {@code true} then Spark serialization (Kryo) will be used to broadcast values
   * in View objects. By default this property is not set, and values are coded using
   * the appropriate {@link Coder}.
   */
  public static final String DIRECT_BROADCAST = "beam.spark.directBroadcast";

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastHelper.class);

  public static <T> BroadcastHelper<T> create(T value, Coder<T> coder) {
    if (Boolean.parseBoolean(System.getProperty(DIRECT_BROADCAST, "false"))) {
      return new DirectBroadcastHelper<>(value);
    }
    return new CodedBroadcastHelper<>(value, coder);
  }

  public abstract T getValue();

  public abstract void broadcast(JavaSparkContext jsc);

  /**
   * A {@link BroadcastHelper} that relies on the underlying
   * Spark serialization (Kryo) to broadcast values. This is appropriate when
   * broadcasting very large values, since no copy of the object is made.
   * @param <T> the type of the value stored in the broadcast variable
   */
  static class DirectBroadcastHelper<T> extends BroadcastHelper<T> {
    private Broadcast<T> bcast;
    private transient T value;

    DirectBroadcastHelper(T value) {
      this.value = value;
    }

    @Override
    public synchronized T getValue() {
      if (value == null) {
        value = bcast.getValue();
      }
      return value;
    }

    @Override
    public void broadcast(JavaSparkContext jsc) {
      this.bcast = jsc.broadcast(value);
    }
  }

  /**
   * A {@link BroadcastHelper} that uses a
   * {@link Coder} to encode values as byte arrays
   * before broadcasting.
   * @param <T> the type of the value stored in the broadcast variable
   */
  static class CodedBroadcastHelper<T> extends BroadcastHelper<T> {
    private Broadcast<byte[]> bcast;
    private final Coder<T> coder;
    private transient T value;

    CodedBroadcastHelper(T value, Coder<T> coder) {
      this.value = value;
      this.coder = coder;
    }

    @Override
    public synchronized T getValue() {
      if (value == null) {
        value = deserialize();
      }
      return value;
    }

    @Override
    public void broadcast(JavaSparkContext jsc) {
      this.bcast = jsc.broadcast(CoderHelpers.toByteArray(value, coder));
    }

    private T deserialize() {
      T val;
      try {
        val = coder.decode(new ByteArrayInputStream(bcast.value()),
            new Coder.Context(true));
      } catch (IOException ioe) {
        // this should not ever happen, log it if it does.
        LOG.warn(ioe.getMessage());
        val = null;
      }
      return val;
    }
  }
}
