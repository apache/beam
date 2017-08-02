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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.ReleaseInfo;

/**
 * Abstract {@link DoFn} that manages {@link Spanner} lifecycle. Use {@link
 * AbstractSpannerFn#databaseClient} to access the Cloud Spanner database client.
 */
abstract class AbstractSpannerFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
  // A common user agent token that indicates that this request was originated from Apache Beam.
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";

  private transient Spanner spanner;
  private transient DatabaseClient databaseClient;

  abstract SpannerConfig getSpannerConfig();

  @Setup
  public void setup() throws Exception {
    SpannerConfig spannerConfig = getSpannerConfig();
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();
    if (spannerConfig.getProjectId() != null) {
      builder.setProjectId(spannerConfig.getProjectId().get());
    }
    if (spannerConfig.getServiceFactory() != null) {
      builder.setServiceFactory(spannerConfig.getServiceFactory());
    }
    ReleaseInfo releaseInfo = ReleaseInfo.getReleaseInfo();
    builder.setUserAgentPrefix(USER_AGENT_PREFIX + "/" + releaseInfo.getVersion());
    SpannerOptions options = builder.build();
    spanner = options.getService();
    databaseClient = spanner.getDatabaseClient(DatabaseId
        .of(options.getProjectId(), spannerConfig.getInstanceId().get(),
            spannerConfig.getDatabaseId().get()));
  }

  @Teardown
  public void teardown() throws Exception {
    if (spanner == null) {
      return;
    }
    spanner.close();
    spanner = null;
  }

  protected DatabaseClient databaseClient() {
    return databaseClient;
  }
}
