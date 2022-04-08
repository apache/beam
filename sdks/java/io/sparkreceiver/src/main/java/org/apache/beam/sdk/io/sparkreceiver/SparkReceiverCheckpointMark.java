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
package org.apache.beam.sdk.io.sparkreceiver;

import java.util.Optional;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;

/** Checkpoint for a {@link SparkReceiverUnboundedReader}. */
@DefaultCoder(AvroCoder.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SparkReceiverCheckpointMark implements UnboundedSource.CheckpointMark {

  private @Nullable String offset;
  private @Nullable Integer position;

  @AvroIgnore
  private Optional<SparkReceiverUnboundedReader<?>>
      reader; // Present when offsets need to be committed.

  private SparkReceiverCheckpointMark() {} // for Avro

  public SparkReceiverCheckpointMark(Integer position, String offset, Optional<SparkReceiverUnboundedReader<?>> reader) {
    this.offset = offset;
    this.position = position;
    this.reader = reader;
  }

  public String getOffset() {
    return offset;
  }

  public Integer getPosition() {
    return position;
  }

  @Override
  public void finalizeCheckpoint() {
    reader.ifPresent(r -> r.finalizeCheckpointMarkAsync(this));
  }

  @Override
  public String toString() {
    return "SparkReceiverCheckpointMark{}";
  }
}
