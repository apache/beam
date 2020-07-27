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
package org.apache.beam.sdk.io.aws2.kinesis;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

/**
 * This class is responsible for establishing the initial set of shards that existed at the given
 * starting point.
 */
class StartingPointShardsFinder implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StartingPointShardsFinder.class);

  /**
   * Finds all the shards at the given startingPoint. This method starts by gathering the oldest
   * shards in the stream and considers them as initial shards set. Then it validates the shards by
   * getting an iterator at the given starting point and trying to read some records. If shard
   * passes the validation then it is added to the result shards set. If not then it is regarded as
   * expired and its successors are taken into consideration. This step is repeated until all valid
   * shards are found.
   *
   * <p>The following diagram depicts sample split and merge operations on a stream with 3 initial
   * shards. Let's consider what happens when T1, T2, T3 or T4 timestamps are passed as the
   * startingPoint.
   *
   * <ul>
   *   <li>T1 timestamp (or TRIM_HORIZON marker) - 0000, 0001 and 0002 shards are the oldest so they
   *       are gathered as initial shards set. All of them are valid at T1 timestamp so they are all
   *       returned from the method.
   *   <li>T2 timestamp - 0000, 0001 and 0002 shards form the initial shards set.
   *       <ul>
   *         <li>0000 passes the validation at T2 timestamp so it is added to the result set
   *         <li>0001 does not pass the validation as it is already closed at T2 timestamp so its
   *             successors 0003 and 0004 are considered. Both are valid at T2 timestamp so they are
   *             added to the resulting set.
   *         <li>0002 also does not pass the validation so its successors 0005 and 0006 are
   *             considered and both are valid.
   *       </ul>
   *       Finally the resulting set contains 0000, 0003, 0004, 0005 and 0006 shards.
   *   <li>T3 timestamp - the beginning is the same as in T2 case.
   *       <ul>
   *         <li>0000 is valid
   *         <li>0001 is already closed at T2 timestamp so its successors 0003 and 0004 are next.
   *             0003 is valid but 0004 is already closed at T3 timestamp. It has one successor 0007
   *             which is the result of merging 0004 and 0005 shards. 0007 has two parent shards
   *             then stored in {@link Shard#parentShardId()} and {@link
   *             Shard#adjacentParentShardId()} fields. Only one of them should follow the relation
   *             to its successor so it is always the shard stored in parentShardId field. Let's
   *             assume that it was 0004 shard and it's the one that considers 0007 its successor.
   *             0007 is valid at T3 timestamp and it's added to the result set.
   *         <li>0002 is closed at T3 timestamp so its successors 0005 and 0006 are next. 0005 is
   *             also closed because it was merged with 0004 shard. Their successor is 0007 and it
   *             was already considered by 0004 shard so no action here is needed. Shard 0006 is
   *             valid.
   *       </ul>
   *   <li>T4 timestamp (or LATEST marker) - following the same reasoning as in previous cases it
   *       end's up with 0000, 0003, 0008 and 0010 shards.
   * </ul>
   *
   * <pre>
   *      T1                T2          T3                      T4
   *      |                 |           |                       |
   * 0000-----------------------------------------------------------
   *
   *
   *             0003-----------------------------------------------
   *            /
   * 0001------+
   *            \
   *             0004-----------+             0008------------------
   *                             \           /
   *                              0007------+
   *                             /           \
   *                  0005------+             0009------+
   *                 /                                   \
   * 0002-----------+                                     0010------
   *                 \                                   /
   *                  0006------------------------------+
   * </pre>
   */
  Set<Shard> findShardsAtStartingPoint(
      SimplifiedKinesisClient kinesis, String streamName, StartingPoint startingPoint)
      throws TransientKinesisException {
    List<Shard> allShards = kinesis.listShards(streamName);
    Set<Shard> initialShards = findInitialShardsWithoutParents(streamName, allShards);

    Set<Shard> startingPointShards = new HashSet<>();
    Set<Shard> expiredShards;
    do {
      Set<Shard> validShards = validateShards(kinesis, initialShards, streamName, startingPoint);
      startingPointShards.addAll(validShards);
      expiredShards = Sets.difference(initialShards, validShards);
      if (!expiredShards.isEmpty()) {
        LOG.info(
            "Following shards expired for {} stream at '{}' starting point: {}",
            streamName,
            startingPoint,
            expiredShards);
      }
      initialShards = findNextShards(allShards, expiredShards);
    } while (!expiredShards.isEmpty());
    return startingPointShards;
  }

  private Set<Shard> findNextShards(List<Shard> allShards, Set<Shard> expiredShards) {
    Set<Shard> nextShards = new HashSet<>();
    for (Shard expiredShard : expiredShards) {
      boolean successorFound = false;
      for (Shard shard : allShards) {
        if (Objects.equals(expiredShard.shardId(), shard.parentShardId())) {
          nextShards.add(shard);
          successorFound = true;
        } else if (Objects.equals(expiredShard.shardId(), shard.adjacentParentShardId())) {
          successorFound = true;
        }
      }
      if (!successorFound) {
        // This can potentially happen during split/merge operation. Newly created shards might be
        // not listed in the allShards list and their predecessor is already considered expired.
        // Retrying should solve the issue.
        throw new IllegalStateException("No successors were found for shard: " + expiredShard);
      }
    }
    return nextShards;
  }

  /**
   * Finds the initial set of shards (the oldest ones). These shards do not have their parents in
   * the shard list.
   */
  private Set<Shard> findInitialShardsWithoutParents(String streamName, List<Shard> allShards) {
    Set<String> shardIds = new HashSet<>();
    for (Shard shard : allShards) {
      shardIds.add(shard.shardId());
    }
    LOG.info("Stream {} has following shards: {}", streamName, shardIds);
    Set<Shard> shardsWithoutParents = new HashSet<>();
    for (Shard shard : allShards) {
      if (!shardIds.contains(shard.parentShardId())) {
        shardsWithoutParents.add(shard);
      }
    }
    return shardsWithoutParents;
  }

  /**
   * Validates the shards at the given startingPoint. Validity is checked by getting an iterator at
   * the startingPoint and then trying to read some records. This action does not affect the records
   * at all. If the shard is valid then it will get read from exactly the same point and these
   * records will be read again.
   */
  private Set<Shard> validateShards(
      SimplifiedKinesisClient kinesis,
      Iterable<Shard> rootShards,
      String streamName,
      StartingPoint startingPoint)
      throws TransientKinesisException {
    Set<Shard> validShards = new HashSet<>();
    ShardIteratorType shardIteratorType =
        ShardIteratorType.fromValue(startingPoint.getPositionName());
    for (Shard shard : rootShards) {
      String shardIterator =
          kinesis.getShardIterator(
              streamName, shard.shardId(), shardIteratorType, null, startingPoint.getTimestamp());
      GetKinesisRecordsResult records =
          kinesis.getRecords(shardIterator, streamName, shard.shardId());
      if (records.getNextShardIterator() != null || !records.getRecords().isEmpty()) {
        validShards.add(shard);
      }
    }
    return validShards;
  }
}
