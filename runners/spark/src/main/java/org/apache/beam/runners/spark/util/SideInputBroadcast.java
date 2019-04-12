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
import org.apache.beam.sdk.coders.Coder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broadcast helper for side inputs. Helps to do the transformation from bytes transform to
 * broadcast transform to value by coder
 */
public class SideInputBroadcast<T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SideInputBroadcast.class);
  private Broadcast<byte[]> bcast;
  private final Coder<T> coder;
  private transient T value;
  private transient byte[] bytes = null;

  private SideInputBroadcast(byte[] bytes, Coder<T> coder) {
    this.bytes = bytes;
    this.coder = coder;
  }

  public static <T> SideInputBroadcast<T> create(byte[] bytes, Coder<T> coder) {
    return new SideInputBroadcast<>(bytes, coder);
  }

  public synchronized T getValue() {
    if (value == null) {
      value = deserialize();
    }
    return value;
  }

  public void broadcast(JavaSparkContext jsc) {
    this.bcast = jsc.broadcast(bytes);
  }

  public void unpersist() {
    this.bcast.unpersist();
  }

  private T deserialize() {
    T val;
    try {
      val = coder.decode(new ByteArrayInputStream(bcast.value()));
    } catch (IOException ioe) {
      // this should not ever happen, log it if it does.
      LOG.warn(ioe.getMessage());
      val = null;
    }
    return val;
  }

  public long getBroadcastSizeEstimate() {
    return SizeEstimator.estimate(bytes);
  }
}
