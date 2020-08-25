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
package org.apache.beam.sdk.io.snowflake.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeStreamingServiceConfig;

/** Fake implementation of {@link SnowflakeService} used in tests. */
public class FakeSnowflakeStreamingServiceImpl
    implements SnowflakeService<SnowflakeStreamingServiceConfig> {
  private FakeSnowflakeIngestManager snowflakeIngestManager;

  @Override
  public void write(SnowflakeStreamingServiceConfig config) throws Exception {
    snowflakeIngestManager = new FakeSnowflakeIngestManager();
    ingest(config);
  }

  @Override
  public String read(SnowflakeStreamingServiceConfig config) throws Exception {
    throw new UnsupportedOperationException("Streaming read is not supported in SnowflakeIO.");
  }

  public void ingest(SnowflakeStreamingServiceConfig config) {
    List<String> rows = new ArrayList<>();
    List<String> filesList = config.getFilesList();
    for (String file : filesList) {
      rows.addAll(readGZIPFile(file.replace("'", "")));
    }

    snowflakeIngestManager.ingestFiles(rows);
  }

  private List<String> readGZIPFile(String file) {
    List<String> lines = new ArrayList<>();
    try {
      GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
      BufferedReader br = new BufferedReader(new InputStreamReader(gzip, Charset.defaultCharset()));

      String line;
      while ((line = br.readLine()) != null) {
        lines.add(line);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read file", e);
    }

    return lines;
  }
}
