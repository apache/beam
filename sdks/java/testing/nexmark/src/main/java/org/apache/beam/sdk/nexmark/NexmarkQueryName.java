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
package org.apache.beam.sdk.nexmark;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Known "Nexmark" queries, some of which are of our own devising but use the same data set. */
@SuppressWarnings("ImmutableEnumChecker")
public enum NexmarkQueryName {
  // A baseline
  PASSTHROUGH(0),

  // The actual Nexmark queries
  CURRENCY_CONVERSION(1),
  SELECTION(2),
  LOCAL_ITEM_SUGGESTION(3),
  AVERAGE_PRICE_FOR_CATEGORY(4),
  HOT_ITEMS(5),
  AVERAGE_SELLING_PRICE_BY_SELLER(6),
  HIGHEST_BID(7),
  MONITOR_NEW_USERS(8), // Query 8

  // Misc other queries against the data set
  WINNING_BIDS(9), // Query "9"
  LOG_TO_SHARDED_FILES(10), // Query "10"
  USER_SESSIONS(11), // Query "11"
  PROCESSING_TIME_WINDOWS(12), // Query "12"

  // Other non-numbered queries
  BOUNDED_SIDE_INPUT_JOIN(13),
  SESSION_SIDE_INPUT_JOIN(14);

  private @Nullable Integer number;

  NexmarkQueryName() {
    this.number = null;
  }

  NexmarkQueryName(int number) {
    this.number = number;
  }

  /**
   * Outputs the number of a query if the query is one that we have given a number, for
   * compatibility. If the query does not have a number, returns its name.
   */
  public String getNumberOrName() {
    if (number != null) {
      return number.toString();
    } else {
      return this.toString();
    }
  }

  /**
   * Parses a number into the corresponding query, using the numbers from the original Nexmark suite
   * and the other numbers that we have added. If the number is not a known query, returns {@code
   * null}.
   */
  public static @Nullable NexmarkQueryName fromNumber(int nexmarkQueryNumber) {
    // Noting that the number of names is O(1) this is still O(1) :-)
    for (NexmarkQueryName queryName : NexmarkQueryName.values()) {
      if (queryName.number == nexmarkQueryNumber) {
        return queryName;
      }
    }
    return null;
  }

  /**
   * @return The given {@link NexmarkQueryName} for the id. The id can be the query number (for
   *     backwards compatibility) or its name.
   */
  public static NexmarkQueryName fromId(String id) {
    NexmarkQueryName query;
    try {
      query = NexmarkQueryName.valueOf(id);
    } catch (IllegalArgumentException exc) {
      query = NexmarkQueryName.fromNumber(Integer.parseInt(id));
    }
    if (query == null) {
      throw new IllegalArgumentException("Unknown query: " + id);
    }
    return query;
  }
}
