/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.runners.spark.util;

import java.io.Serializable;
import java.util.Arrays;

//todo predelat Serializable na kryo
public class CompositeKey implements Serializable {


  private final byte[] key;
  private final byte[] window ;
  private final long timestamp;


  public CompositeKey(byte[] key, byte[] window, long timestamp) {
    this.key = key;
    this.window = window;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositeKey that = (CompositeKey) o;
    return Arrays.equals(key, that.key) &&
        Arrays.equals(window, that.window) &&
        timestamp == that.timestamp;
  }

  public boolean isSameKey(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositeKey that = (CompositeKey) o;
    return Arrays.equals(key, that.key) &&
        Arrays.equals(window, that.window);
  }

  @Override
  public int hashCode() {
    int result =  Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(window);
    return result;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getWindow() {
    return window;
  }

  public long getTimestamp() {
    return timestamp;
  }
}