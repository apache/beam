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

/** Describes move-out of the key ranges into the change stream partition. */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(AvroCoder.class)
public class MoveOutEvent implements Serializable {

  private static final long serialVersionUID = 1961571231618408326L;

  private String destinationPartitionToken;

  /** Default constructor for serialization only. */
  private MoveOutEvent() {}

  /**
   * Constructs a MoveOutEvent from the destination partition token.
   *
   * @param destinationPartitionToken An unique partition identifier describing the destination
   *     change stream partition that recorded changes for the key range that is moving into this
   *     partition.
   */
  public MoveOutEvent(String destinationPartitionToken) {
    this.destinationPartitionToken = destinationPartitionToken;
  }

  /**
   * The destination change stream partition token that recorded changes for the key range that is
   * moving out this partition.
   *
   * @return destination partition token string.
   */
  public String getDestinationPartitionToken() {
    return destinationPartitionToken;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MoveOutEvent)) {
      return false;
    }
    MoveOutEvent moveOutEvent = (MoveOutEvent) o;
    return Objects.equals(destinationPartitionToken, moveOutEvent.destinationPartitionToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(destinationPartitionToken);
  }

  @Override
  public String toString() {
    return "MoveOutEvent{" + "destinationPartitionToken=" + destinationPartitionToken + '}';
  }
}
