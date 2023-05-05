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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getIntersectingPartition;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

public class ChangeStreamContinuationTokenHelper {
  /**
   * Return the continuation token with correct partition. The partition in the
   * ChangeStreamContinuationToken for merges is not the correct partition (this is a backend bug
   * that is being fixed). The partition currently represents the child partition, where the current
   * partition should merge to.
   *
   * <p>For example: Partition [A, B) gets CloseStream to merge into [A, C). The
   * ChangeStreamContinuationToken returned is <code>{ partition = [A, C), token = "token1" }</code>
   * . However, the correct ChangeStreamContinuationToken should be <code>
   * { partition = [A, B), token = "token1" }</code>.
   *
   * @param parentPartition parent partition where the ChangeStreamContinuationToken is generated
   * @param token ChangeStreamContinuationToken to be fixed
   * @return ChangeStreamContinuationToken with correct partition field
   */
  public static ChangeStreamContinuationToken getTokenWithCorrectPartition(
      ByteStringRange parentPartition, ChangeStreamContinuationToken token)
      throws IllegalArgumentException {
    return ChangeStreamContinuationToken.create(
        getIntersectingPartition(parentPartition, token.getPartition()), token.getToken());
  }
}
