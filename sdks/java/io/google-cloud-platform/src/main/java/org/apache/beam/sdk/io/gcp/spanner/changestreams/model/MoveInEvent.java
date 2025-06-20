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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** Describes move-in of the key ranges into the change stream partition. */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class MoveInEvent implements Serializable {

  private static final long serialVersionUID = 7954905180615424094L;

  private String sourcePartitionToken;

  /** Default constructor for serialization only. */
  private MoveInEvent() {}

  /**
   * Constructs a MoveInEvent from the source partition token.
   *
   * @param sourcePartitionToken An unique partition identifier describing the source change stream
   *     partition that recorded changes for the key range that is moving into this partition.
   */
  public MoveInEvent(String sourcePartitionToken) {
    this.sourcePartitionToken = sourcePartitionToken;
  }

  /**
   * The source change stream partition token that recorded changes for the key range that is moving
   * into this partition.
   *
   * @return source partition token string.
   */
  public String getSourcePartitionToken() {
    return sourcePartitionToken;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MoveInEvent)) {
      return false;
    }
    MoveInEvent moveInEvent = (MoveInEvent) o;
    return Objects.equals(sourcePartitionToken, moveInEvent.sourcePartitionToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourcePartitionToken);
  }

  @Override
  public String toString() {
    return "MoveInEvent{" + "sourcePartitionToken=" + sourcePartitionToken + '}';
  }
}
