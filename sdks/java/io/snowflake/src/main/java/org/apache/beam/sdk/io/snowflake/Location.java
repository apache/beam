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
package org.apache.beam.sdk.io.snowflake;

import java.io.Serializable;

/**
 * Used as one of the arguments for {@link org.apache.beam.sdk.io.snowflake.SnowflakeIO} write and
 * read operations. It keeps information about storage integration and staging bucket name.
 * Integration name is Snowflake storage integration object created according to Snowflake
 * documentation for the GCS bucket. Staging bucket name is Google Cloud Storage bucket which in the
 * case of writing operation will be used to save CSV files which will end up in Snowflake under
 * “staging_bucket_name/data” path and in the case of reading operation will be used as a temporary
 * location for storing CSV files named `sf_copy_csv_DATE_TIME_RANDOMSUFFIX` which will be removed
 * automatically once Read operation finishes.
 */
public class Location implements Serializable {
  private String storageIntegrationName;
  private String stagingBucketName;

  public static Location of(SnowflakePipelineOptions options) {
    return new Location(options.getStorageIntegrationName(), options.getStagingBucketName());
  }

  public static Location of(String storageIntegrationName, String stagingBucketName) {
    return new Location(storageIntegrationName, stagingBucketName);
  }

  private Location(String storageIntegrationName, String stagingBucketName) {
    this.storageIntegrationName = storageIntegrationName;
    this.stagingBucketName = stagingBucketName;
  }

  public String getStorageIntegrationName() {
    return storageIntegrationName;
  }

  public String getStagingBucketName() {
    return stagingBucketName;
  }

  public boolean isStorageIntegrationName() {
    return storageIntegrationName != null;
  }
}
