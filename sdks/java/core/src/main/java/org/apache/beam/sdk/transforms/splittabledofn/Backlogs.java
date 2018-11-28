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
package org.apache.beam.sdk.transforms.splittabledofn;

/** Definitions and convenience methods for reporting/consuming/updating backlogs. */
public final class Backlogs {
  /**
   * {@link RestrictionTracker}s which can provide a backlog should implement this interface.
   * Implementations that do not implement this interface will be assumed to have an unknown
   * backlog.
   *
   * <p>By default, the backlog partition identifier is represented as the encoded element and
   * restriction pair. See {@link HasPartitionedBacklog} for {@link RestrictionTracker}s that report
   * backlogs over a shared resource.
   */
  public interface HasBacklog {
    Backlog getBacklog();
  }

  /**
   * {@link RestrictionTracker}s which can provide a backlog that is from a shared resource such as
   * a message queue should implement this interface to provide the partition identifier. The
   * partition identifier is used by runners during backlog aggregation.
   *
   * <p>This allows runners to understand as to how to aggregate backlogs.
   *
   * <p>Returns the partition identifier.
   */
  public interface HasPartitionedBacklog extends HasBacklog {
    byte[] getBacklogPartition();
  }
}
