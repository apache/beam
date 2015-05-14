/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;

import com.google.cloud.dataflow.sdk.coders.Coder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BroadcastHelper<T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastHelper.class);

  private Broadcast<byte[]> bcast;
  private final Coder<T> coder;
  private transient T value;

  BroadcastHelper(T value, Coder<T> coder) {
    this.value = value;
    this.coder = coder;
  }

  public synchronized T getValue() {
    if (value == null) {
      value = deserialize();
    }
    return value;
  }

  public void broadcast(JavaSparkContext jsc) {
    this.bcast = jsc.broadcast(CoderHelpers.toByteArray(value, coder));
  }

  private T deserialize() {
    T val;
    try {
      val = coder.decode(new ByteArrayInputStream(bcast.value()), new Coder.Context(true));
    } catch (IOException ioe) {
      // this should not ever happen, log it if it does.
      LOG.warn(ioe.getMessage());
      val = null;
    }
    return val;
  }
}
