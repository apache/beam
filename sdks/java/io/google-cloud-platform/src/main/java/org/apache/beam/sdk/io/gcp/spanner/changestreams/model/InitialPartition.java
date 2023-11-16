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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import java.util.HashSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/**
 * Utility class to determine initial partition constants and methods.
 *
 * <p>The initial partition has the artificial token defined by {@link
 * InitialPartition#PARTITION_TOKEN} and it has no parent tokens.
 */
public class InitialPartition {

  /**
   * The token of the initial partition. This is an artificial token for the Connector and it is not
   * recognised by Cloud Spanner.
   */
  public static final String PARTITION_TOKEN = "Parent0";
  /** The empty set representing the initial partition parent tokens. */
  public static final HashSet<String> PARENT_TOKENS = Sets.newHashSet();

  /**
   * Verifies if the given partition token is the initial partition.
   *
   * @param partitionToken the partition token to be checked
   * @return true if the given token is the initial partition, and false otherwise
   */
  public static boolean isInitialPartition(String partitionToken) {
    return PARTITION_TOKEN.equals(partitionToken);
  }
}
