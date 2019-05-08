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
 * This class generates hash-based unique ids from String. The id is shorter to use in states and
 * repartition streams.
 */
public class HashIdGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(HashIdGenerator.class);

  private static final int MAX_ID_LENGTH = 5;
  private final Set<String> usedIds = new HashSet<>();

  public String getId(String name) {
    // Use the id directly if it is unique and the length is less than max
    if (name.length() <= MAX_ID_LENGTH && !usedIds.contains(name)) {
      return name;
    }

    // Pick the last 4 bytes of hashcode and use hex format
    final String origId = Integer.toHexString(name.hashCode() & 0xffff);
    String id = origId;
    int suffixNum = 2;
    while (true) {
      if (usedIds.add(id)) {
        LOG.info("Name {} is mapped to id {}", name, id);
        return id;
      }
      // A duplicate!  Retry.
      id = origId + "-" + suffixNum++;
    }
  }
}
