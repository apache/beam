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
package org.apache.beam.sdk.io.snowflake.services;

import java.util.List;
import net.snowflake.ingest.SimpleIngestManager;

/** Class for preparing configuration for streaming write. */
public class SnowflakeStreamingServiceConfig extends ServiceConfig {
  private SimpleIngestManager ingestManager;
  private List<String> filesList;
  private String stagingBucketDir;

  /**
   * Constructor to create configuration for streaming write.
   *
   * @param filesList list of strings of staged files' names.
   * @param stagingBucketDir name of a bucket and directory inside where files are staged and awaits
   *     for being loaded to Snowflake.
   * @param ingestManager instance of {@link SimpleIngestManager}.
   */
  public SnowflakeStreamingServiceConfig(
      List<String> filesList, String stagingBucketDir, SimpleIngestManager ingestManager) {
    this.filesList = filesList;
    this.stagingBucketDir = stagingBucketDir;
    this.ingestManager = ingestManager;
  }

  /**
   * Getter for ingest manager which serves API to load data in streaming mode and retrieve a report
   * about loaded data.
   *
   * @return instance of {@link SimpleIngestManager}.
   */
  public SimpleIngestManager getIngestManager() {
    return ingestManager;
  }

  /**
   * Getter for a list of staged files which are will be loaded to Snowflake.
   *
   * @return list of strings of staged files' names.
   */
  public List<String> getFilesList() {
    return filesList;
  }

  /**
   * Getter for a bucket name with directory where files were staged and waiting for loading.
   *
   * @return name of a bucket and directory inside in form {@code gs://mybucket/dir/}
   */
  public String getStagingBucketDir() {
    return stagingBucketDir;
  }
}
