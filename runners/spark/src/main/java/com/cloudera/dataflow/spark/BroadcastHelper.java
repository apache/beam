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
import org.apache.spark.broadcast.Broadcast;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;

class BroadcastHelper<T> implements Serializable {
  private final Broadcast<byte[]> bcast;
  private final Coder<T> coder;
  private transient T value;

  BroadcastHelper(Broadcast<byte[]> bcast, Coder<T> coder) {
    this.bcast = bcast;
    this.coder = coder;
  }

  public synchronized T getValue() {
    if (value == null) {
      value = deserialize();
    }
    return value;
  }

  private T deserialize() {
    try {
      return coder.decode(new ByteArrayInputStream(bcast.value()), new Coder.Context(true));
    } catch (IOException e) {
      throw new RuntimeException("Error deserializing broadcast variable", e);
    }
  }
}
