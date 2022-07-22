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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

/** This enum contains the states that PartitionRestrictionTracker will go through. */
public enum PartitionMode {
  // In this state, the restriction tracker will update the state of the input partition token
  // from SCHEDULED to RUNNING.
  UPDATE_STATE,
  // In this state, the restriction tracker will execute a change stream query.
  QUERY_CHANGE_STREAM,
  // In this state, the restriction tracker will wait for the child partition SDFs to start
  // running before terminating the SDF.
  WAIT_FOR_CHILD_PARTITIONS,
  // In this state, the restriction tracker will terminate the SDF.
  DONE,

  // Occurs when Dataflow checkpoints the current restriction.
  STOP
}
