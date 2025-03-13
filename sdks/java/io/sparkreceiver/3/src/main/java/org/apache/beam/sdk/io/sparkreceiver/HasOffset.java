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

/**
 * Interface for any Spark {@link org.apache.spark.streaming.receiver.Receiver} that supports
 * reading from and to some offset.
 */
public interface HasOffset {

  /** @param offset inclusive start offset from which the reading should be started. */
  void setStartOffset(Long offset);

  /** @return exclusive end offset to which the reading from current page will occur. */
  Long getEndOffset();

  /**
   * Some {@link org.apache.spark.streaming.receiver.Receiver} support mechanism of checkpoint (e.g.
   * ack). This method should be called before stopping the receiver.
   */
  default void setCheckpoint(Long recordsProcessed) {};
}
