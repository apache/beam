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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.value.AutoValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.TextFormat;

@AutoValue
public abstract class WindmillComputationKey {

  public static WindmillComputationKey create(
      String computationId, ByteString key, long shardingKey) {
    return new AutoValue_WindmillComputationKey(computationId, key, shardingKey);
  }

  public abstract String computationId();

  public abstract ByteString key();

  public abstract long shardingKey();

  @Override
  public String toString() {
    return String.format(
        "%s: %s-%d", computationId(), TextFormat.escapeBytes(key()), shardingKey());
  }
}
