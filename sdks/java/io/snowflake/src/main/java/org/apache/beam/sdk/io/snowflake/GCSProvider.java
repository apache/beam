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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * Implemenation of {@link org.apache.beam.sdk.io.snowflake.SnowflakeCloudProvider} used in
 * production.
 */
public class GCSProvider implements SnowflakeCloudProvider, Serializable {
  private static final String SF_PREFIX = "gcs://";
  private static final String GCS_PREFIX = "gs://";

  @Override
  public void removeFiles(String stagingBucketName, String pathOnBucket) throws IOException {
    String combinedPath = GCS_PREFIX + stagingBucketName + '/' + pathOnBucket + "/*";
    List<ResourceId> paths =
        FileSystems.match(combinedPath).metadata().stream()
            .map(metadata -> metadata.resourceId())
            .collect(Collectors.toList());

    FileSystems.delete(paths);
  }

  @Override
  public String formatCloudPath(String... pathSteps) {
    StringBuilder builder = new StringBuilder();
    builder.append(SF_PREFIX);

    for (String step : pathSteps) {
      builder.append(String.format("%s/", step));
    }

    return builder.toString();
  }

  @Override
  public String transformSnowflakePathToCloudPath(String path) {
    return path.replace(SF_PREFIX, GCS_PREFIX);
  }
}
