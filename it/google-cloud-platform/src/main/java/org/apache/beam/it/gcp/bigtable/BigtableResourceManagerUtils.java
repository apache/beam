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
package org.apache.beam.it.gcp.bigtable;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

import com.google.cloud.bigtable.admin.v2.models.StorageType;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Utilities for {@link BigtableResourceManager} implementations. */
public final class BigtableResourceManagerUtils {

  private static final int MAX_CLUSTER_ID_LENGTH = 30;
  private static final Pattern ILLEGAL_CLUSTER_CHARS = Pattern.compile("[^a-z0-9-]");
  private static final String REPLACE_CLUSTER_CHAR = "-";
  public static final int MAX_INSTANCE_ID_LENGTH = 30;
  private static final Pattern ILLEGAL_INSTANCE_ID_CHARS = Pattern.compile("[^a-z0-9-]");
  private static final String REPLACE_INSTANCE_ID_CHAR = "-";
  private static final int MIN_TABLE_ID_LENGTH = 1;
  private static final int MAX_TABLE_ID_LENGTH = 40;
  private static final Pattern ILLEGAL_TABLE_CHARS = Pattern.compile("[^a-zA-Z0-9-_.]");
  private static final String REPLACE_TABLE_ID_CHAR = "-";

  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS");

  private BigtableResourceManagerUtils() {}

  /**
   * Generates a collection that contains a single BigtableResourceManagerCluster object using the
   * given parameters.
   *
   * @param baseString the test id to associate the bigtable cluster to.
   * @param zone the zone/region that the cluster will be deployed to.
   * @param numNodes the number of nodes that the cluster will contain.
   * @param storageType the type of storage to configure the cluster with (SSD or HDD).
   * @return List containing a single BigtableResourceManagerCluster object.
   */
  static List<BigtableResourceManagerCluster> generateDefaultClusters(
      String baseString, String zone, int numNodes, StorageType storageType) {

    String clusterId =
        generateResourceId(
            baseString.toLowerCase(),
            ILLEGAL_CLUSTER_CHARS,
            REPLACE_CLUSTER_CHAR,
            MAX_CLUSTER_ID_LENGTH,
            TIME_FORMAT);
    BigtableResourceManagerCluster cluster =
        BigtableResourceManagerCluster.create(clusterId, zone, numNodes, storageType);

    return ImmutableList.of(cluster);
  }

  /**
   * Generates an instance id from a given string.
   *
   * @param baseString The string to generate the id from.
   * @return The instance id string.
   */
  static String generateInstanceId(String baseString) {
    return generateResourceId(
        baseString.toLowerCase(),
        ILLEGAL_INSTANCE_ID_CHARS,
        REPLACE_INSTANCE_ID_CHAR,
        MAX_INSTANCE_ID_LENGTH,
        TIME_FORMAT);
  }

  /**
   * Generates a table id from a given string.
   *
   * @param baseString The string to generate the id from.
   * @return The instance id string.
   */
  public static String generateTableId(String baseString) {
    return generateResourceId(
        baseString.toLowerCase(),
        ILLEGAL_TABLE_CHARS,
        REPLACE_TABLE_ID_CHAR,
        MAX_TABLE_ID_LENGTH,
        TIME_FORMAT);
  }

  /**
   * Checks whether the given table ID is valid according to GCP constraints.
   *
   * @param idToCheck the table ID to check.
   * @throws IllegalArgumentException if the table ID is invalid.
   */
  static void checkValidTableId(String idToCheck) {
    if (idToCheck.length() < MIN_TABLE_ID_LENGTH) {
      throw new IllegalArgumentException("Table ID " + idToCheck + " cannot be empty.");
    }
    if (idToCheck.length() > MAX_TABLE_ID_LENGTH) {
      throw new IllegalArgumentException(
          "Table ID "
              + idToCheck
              + " cannot be longer than "
              + MAX_TABLE_ID_LENGTH
              + " characters.");
    }
    if (ILLEGAL_TABLE_CHARS.matcher(idToCheck).find()) {
      throw new IllegalArgumentException(
          "Table ID "
              + idToCheck
              + " is not a valid ID. Only letters, numbers, hyphens, underscores and exclamation points are allowed.");
    }
  }
}
