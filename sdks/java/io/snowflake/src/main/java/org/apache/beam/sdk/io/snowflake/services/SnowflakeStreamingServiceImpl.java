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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.IngestResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implemenation of {@link SnowflakeService} used in production. */
public class SnowflakeStreamingServiceImpl
    implements SnowflakeService<SnowflakeStreamingServiceConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeStreamingServiceImpl.class);
  private transient SimpleIngestManager ingestManager;

  /** Writing data to Snowflake in streaming mode. */
  @Override
  public void write(SnowflakeStreamingServiceConfig config) throws Exception {
    ingest(config);
  }

  /** Reading data from Snowflake in streaming mode is not supported. */
  @Override
  public String read(SnowflakeStreamingServiceConfig config) throws Exception {
    throw new UnsupportedOperationException("Not supported by SnowflakeIO.");
  }

  /**
   * SnowPipe is processing files from stage in streaming mode.
   *
   * @param config configuration object containing parameters for writing files to Snowflake
   * @throws IngestResponseException REST API response error
   * @throws IOException Snowflake problem while streaming
   * @throws URISyntaxException creating request error
   */
  private void ingest(SnowflakeStreamingServiceConfig config)
      throws IngestResponseException, IOException, URISyntaxException {
    List<String> filesList = config.getFilesList();
    String stagingBucketDir = config.getStagingBucketDir();
    ingestManager = config.getIngestManager();

    Set<String> files =
        filesList.stream()
            .map(e -> e.replaceAll(String.valueOf(stagingBucketDir), ""))
            .map(e -> e.replaceAll("'", ""))
            .collect(Collectors.toSet());

    if (!files.isEmpty()) {
      this.ingestManager.ingestFiles(SimpleIngestManager.wrapFilepaths(files), null);
    }
  }
}
