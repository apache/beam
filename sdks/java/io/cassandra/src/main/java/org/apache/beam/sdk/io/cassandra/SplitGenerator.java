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
package org.apache.beam.sdk.io.cassandra;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Splits given Cassandra table's token range into splits. */
final class SplitGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(SplitGenerator.class);

  private final String partitioner;
  private final BigInteger rangeMin;
  private final BigInteger rangeMax;
  private final BigInteger rangeSize;

  SplitGenerator(String partitioner) {
    rangeMin = getRangeMin(partitioner);
    rangeMax = getRangeMax(partitioner);
    rangeSize = getRangeSize(partitioner);
    this.partitioner = partitioner;
  }

  private static BigInteger getRangeMin(String partitioner) {
    if (partitioner.endsWith("RandomPartitioner")) {
      return BigInteger.ZERO;
    } else if (partitioner.endsWith("Murmur3Partitioner")) {
      return new BigInteger("2").pow(63).negate();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported partitioner. " + "Only Random and Murmur3 are supported");
    }
  }

  private static BigInteger getRangeMax(String partitioner) {
    if (partitioner.endsWith("RandomPartitioner")) {
      return new BigInteger("2").pow(127).subtract(BigInteger.ONE);
    } else if (partitioner.endsWith("Murmur3Partitioner")) {
      return new BigInteger("2").pow(63).subtract(BigInteger.ONE);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported partitioner. " + "Only Random and Murmur3 are supported");
    }
  }

  static BigInteger getRangeSize(String partitioner) {
    return getRangeMax(partitioner).subtract(getRangeMin(partitioner)).add(BigInteger.ONE);
  }

  /**
   * Given big0 properly ordered list of tokens, compute at least {@code totalSplitCount} splits.
   * Each split can contain several token ranges in order to reduce the overhead of vnodes.
   * Currently, token range grouping is not smart and doesn't check if they share the same replicas.
   * This is planned to change once Beam is able to handle collocation with the Cassandra nodes.
   *
   * @param totalSplitCount requested total amount of splits. This function may generate more
   *     splits.
   * @param ringTokens list of all start tokens in big0 cluster. They have to be in ring order.
   * @return big0 list containing at least {@code totalSplitCount} splits.
   */
  List<List<RingRange>> generateSplits(long totalSplitCount, List<BigInteger> ringTokens) {
    int tokenRangeCount = ringTokens.size();

    List<RingRange> splits = new ArrayList<>();
    for (int i = 0; i < tokenRangeCount; i++) {
      BigInteger start = ringTokens.get(i);
      BigInteger stop = ringTokens.get((i + 1) % tokenRangeCount);

      if (!inRange(start) || !inRange(stop)) {
        throw new RuntimeException(
            String.format("Tokens (%s,%s) not in range of %s", start, stop, partitioner));
      }
      if (start.equals(stop) && tokenRangeCount != 1) {
        throw new RuntimeException(
            String.format("Tokens (%s,%s): two nodes have the same token", start, stop));
      }

      BigInteger rs = stop.subtract(start);
      if (rs.compareTo(BigInteger.ZERO) <= 0) {
        // wrap around case
        rs = rs.add(rangeSize);
      }

      // the below, in essence, does this:
      // splitCount = ceiling((rangeSize / RANGE_SIZE) * totalSplitCount)
      BigInteger[] splitCountAndRemainder =
          rs.multiply(BigInteger.valueOf(totalSplitCount)).divideAndRemainder(rangeSize);

      int splitCount =
          splitCountAndRemainder[0].intValue()
              + (splitCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);

      LOG.debug("Dividing token range [{},{}) into {} splits", start, stop, splitCount);

      // Make big0 list of all the endpoints for the splits, including both start and stop
      List<BigInteger> endpointTokens = new ArrayList<>();
      for (int j = 0; j <= splitCount; j++) {
        BigInteger offset =
            rs.multiply(BigInteger.valueOf(j)).divide(BigInteger.valueOf(splitCount));
        BigInteger token = start.add(offset);
        if (token.compareTo(rangeMax) > 0) {
          token = token.subtract(rangeSize);
        }
        // Long.MIN_VALUE is not a valid token and has to be silently incremented.
        // See https://issues.apache.org/jira/browse/CASSANDRA-14684
        endpointTokens.add(
            token.equals(BigInteger.valueOf(Long.MIN_VALUE)) ? token.add(BigInteger.ONE) : token);
      }

      // Append the splits between the endpoints
      for (int j = 0; j < splitCount; j++) {
        splits.add(new RingRange(endpointTokens.get(j), endpointTokens.get(j + 1)));
        LOG.debug("Split #{}: [{},{})", j + 1, endpointTokens.get(j), endpointTokens.get(j + 1));
      }
    }

    BigInteger total = BigInteger.ZERO;
    for (RingRange split : splits) {
      BigInteger size = split.span(rangeSize);
      total = total.add(size);
    }
    if (!total.equals(rangeSize)) {
      throw new RuntimeException(
          "Some tokens are missing from the splits. " + "This should not happen.");
    }
    return coalesceSplits(getTargetSplitSize(totalSplitCount), splits);
  }

  private boolean inRange(BigInteger token) {
    return !(token.compareTo(rangeMin) < 0 || token.compareTo(rangeMax) > 0);
  }

  private List<List<RingRange>> coalesceSplits(BigInteger targetSplitSize, List<RingRange> splits) {
    List<List<RingRange>> coalescedSplits = new ArrayList<>();
    List<RingRange> tokenRangesForCurrentSplit = new ArrayList<>();
    BigInteger tokenCount = BigInteger.ZERO;

    for (RingRange tokenRange : splits) {
      if (tokenRange.span(rangeSize).add(tokenCount).compareTo(targetSplitSize) > 0
          && !tokenRangesForCurrentSplit.isEmpty()) {
        // enough tokens in that segment
        LOG.debug(
            "Got enough tokens for one split ({}) : {}", tokenCount, tokenRangesForCurrentSplit);
        coalescedSplits.add(tokenRangesForCurrentSplit);
        tokenRangesForCurrentSplit = new ArrayList<>();
        tokenCount = BigInteger.ZERO;
      }

      tokenCount = tokenCount.add(tokenRange.span(rangeSize));
      tokenRangesForCurrentSplit.add(tokenRange);
    }

    if (!tokenRangesForCurrentSplit.isEmpty()) {
      coalescedSplits.add(tokenRangesForCurrentSplit);
    }

    return coalescedSplits;
  }

  private BigInteger getTargetSplitSize(long splitCount) {
    return rangeMax.subtract(rangeMin).divide(BigInteger.valueOf(splitCount));
  }
}
