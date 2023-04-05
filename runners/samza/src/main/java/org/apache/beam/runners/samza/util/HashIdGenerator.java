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
package org.apache.beam.runners.samza.util;

import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates hash-based unique ids from String. The id length is the hash length and the
 * suffix length combined. Ids generated are guaranteed to be unique, such that same names will be
 * hashed to different ids.
 */
public class HashIdGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(HashIdGenerator.class);

  private static final int DEFAULT_MAX_HASH_LENGTH = 5;
  private final int maxHashLength;
  private final Set<String> usedIds = new HashSet<>();

  public HashIdGenerator(int maxHashLength) {
    this.maxHashLength = maxHashLength;
  }

  public HashIdGenerator() {
    this(DEFAULT_MAX_HASH_LENGTH);
  }

  public String getId(String name) {
    // Use the id directly if it is unique and the length is less than max
    if (name.length() <= maxHashLength && usedIds.add(name)) {
      return name;
    }

    // Pick the last bytes of hashcode and use hex format
    final String hexString = Integer.toHexString(name.hashCode());
    final String origId =
        hexString.length() <= maxHashLength
            ? hexString
            : hexString.substring(Math.max(0, hexString.length() - maxHashLength));
    String id = origId;
    int suffixNum = 2;
    while (!usedIds.add(id)) {
      // A duplicate!  Retry.
      id = origId + "-" + suffixNum++;
    }
    LOG.info("Name {} is mapped to id {}", name, id);
    return id;
  }
}
