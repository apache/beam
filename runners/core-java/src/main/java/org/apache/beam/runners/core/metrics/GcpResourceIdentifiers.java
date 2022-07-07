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
package org.apache.beam.runners.core.metrics;

/**
 * Helper functions to generate resource labels strings for GCP entitites These can be used on
 * MonitoringInfo 'resource' labels. See example entities:
 *
 * <p>https://s.apache.org/beam-gcp-debuggability For GCP entities, populate the RESOURCE label with
 * the aip.dev/122 format: https://google.aip.dev/122 If an official GCP format does not exist, try
 * to use the following format. //whatever.googleapis.com/parents/{parentId}/whatevers/{whateverId}
 */
public class GcpResourceIdentifiers {

  public static String bigQueryTable(String projectId, String datasetId, String tableId) {
    return String.format(
        "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
        projectId, datasetId, tableId);
  }

  public static String bigtableTableID(String project, String instance, String table) {
    return String.format("projects/%s/instances/%s/tables/%s", project, instance, table);
  }

  public static String bigtableResource(String projectId, String instanceId, String tableId) {
    return String.format(
        "//bigtable.googleapis.com/projects/%s/instances/%s/tables/%s",
        projectId, instanceId, tableId);
  }

  public static String cloudStorageBucket(String bucketId) {
    return String.format("//storage.googleapis.com/buckets/%s", bucketId);
  }

  public static String datastoreResource(String projectId, String namespace) {
    return String.format(
        "//bigtable.googleapis.com/projects/%s/namespaces/%s", projectId, namespace);
  }

  public static String spannerTable(
      String projectId, String instanceId, String databaseId, String tableId) {
    return String.format(
        "//spanner.googleapis.com/projects/%s/instances/%s/databases/%s/tables/%s",
        projectId, instanceId, databaseId, tableId);
  }

  public static String spannerQuery(
      String projectId, String instanceId, String databaseId, String queryName) {
    return String.format(
        "//spanner.googleapis.com/projects/%s/instances/%s/databases/%s/queries/%s",
        projectId, instanceId, databaseId, queryName);
  }
}
