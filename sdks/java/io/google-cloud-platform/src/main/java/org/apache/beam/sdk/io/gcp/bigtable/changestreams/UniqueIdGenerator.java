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

import java.security.SecureRandom;
import java.util.Base64;
import org.apache.beam.sdk.annotations.Internal;

/** Generate unique IDs that can be used to differentiate different jobs and partitions. */
@Internal
public class UniqueIdGenerator {
  private static final SecureRandom secureRandom = new SecureRandom();

  /**
   * Return a random base64 encoded 8 byte string. This is used to identify streamer of a specific
   * partition. We expect there to be at most single digit duplicates of the same partition at any
   * one time. The odd of collision if we generate <10 values per partition is less than 1 in 10^18.
   *
   * @return a random 8 byte string.
   */
  public static String getNextId() {
    byte[] bytes = new byte[8];
    secureRandom.nextBytes(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }

  /**
   * Return a random base64 encoded 8 byte string. This is used to identify a pipeline. Once a
   * pipeline is complete, the metadata is left there and not cleaned up. It's possible, over the
   * lifetime, there to be thousands and more pipelines in a single metadata table. The odds of
   * collision if we expect 100,000 pipelines is less than 1 in 1 billion.
   *
   * @return a random 8 byte string.
   */
  public static String generateRowKeyPrefix() {
    byte[] bytes = new byte[8];
    secureRandom.nextBytes(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }
}
