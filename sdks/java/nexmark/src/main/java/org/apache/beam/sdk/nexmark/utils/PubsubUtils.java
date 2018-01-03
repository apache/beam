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

package org.apache.beam.sdk.nexmark.utils;

import com.google.common.base.Strings;

import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;

/**
 * Pubsub topic names utils.
 */
public class PubsubUtils {
  /**
   * Return a subscription name.
   */
  public static String shortSubscription(NexmarkOptions options, String queryName, long now) {

    String baseSubscription = options.getPubsubSubscription();
    if (Strings.isNullOrEmpty(baseSubscription)) {
      throw new RuntimeException("Missing --pubsubSubscription");
    }

    return formatSubscription(options.getResourceNameMode(), baseSubscription, queryName, now);
  }

  /**
   * Return a topic name.
   */
  public static String shortTopic(NexmarkOptions options, String queryName, long now) {

    String baseTopic = options.getPubsubTopic();
    if (Strings.isNullOrEmpty(baseTopic)) {
      throw new RuntimeException("Missing --pubsubTopic");
    }
    return formatSubscription(options.getResourceNameMode(), baseTopic, queryName, now);
  }

  private static String formatSubscription(
      NexmarkUtils.ResourceNameMode namingMode,
      String baseName,
      String queryName,
      long now) {

    switch (namingMode) {
      case VERBATIM:
        return baseName;
      case QUERY:
        return String.format("%s_%s_source", baseName, queryName);
      case QUERY_AND_SALT:
        return String.format("%s_%s_%d_source", baseName, queryName, now);
      default:
        throw new RuntimeException("Unrecognized enum " + namingMode);
    }
  }
}
