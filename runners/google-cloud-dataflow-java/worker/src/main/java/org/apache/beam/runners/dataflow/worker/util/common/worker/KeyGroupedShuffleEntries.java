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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/** A collection of ShuffleEntries, all with the same key. */
public class KeyGroupedShuffleEntries {
  public final ShufflePosition position;
  public final ByteString key;
  public final Reiterable<ShuffleEntry> values;

  public KeyGroupedShuffleEntries(
      ShufflePosition position, ByteString key, Reiterable<ShuffleEntry> values) {
    this.position = position;
    this.key = key;
    this.values = values;
  }
}
