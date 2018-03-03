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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;

/**
 * Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances.
 */
public class SpannerAccessor implements AutoCloseable {
  private final Spanner spanner;
  private final DatabaseClient databaseClient;
  private final BatchClient batchClient;

  SpannerAccessor(Spanner spanner, DatabaseClient databaseClient, BatchClient batchClient) {
    this.spanner = spanner;
    this.databaseClient = databaseClient;
    this.batchClient = batchClient;
  }

  public DatabaseClient getDatabaseClient() {

    return databaseClient;
  }
  public BatchClient getBatchClient() {
    return batchClient;
  }

  @Override
  public void close() {
    spanner.close();
  }
}
