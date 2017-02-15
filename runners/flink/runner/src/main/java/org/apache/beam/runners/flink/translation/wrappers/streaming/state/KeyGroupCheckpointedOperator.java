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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import java.io.DataOutputStream;

/**
 * This interface is used to checkpoint key-groups state.
 */
public interface KeyGroupCheckpointedOperator extends KeyGroupRestoringOperator{
  /**
   * Snapshots the state for a given {@code keyGroupIdx}.
   *
   * <p>AbstractStreamOperator would call this hook in
   * AbstractStreamOperator.snapshotState() while iterating over the key groups.
   * @param keyGroupIndex the id of the key-group to be put in the snapshot.
   * @param out the stream to write to.
   */
  void snapshotKeyGroupState(int keyGroupIndex, DataOutputStream out) throws Exception;
}
