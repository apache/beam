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

import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** Helper methods for Spanner IT tests to support Spanner Omni natively. */
public class SpannerTestHelper {

  public static String getOmniEndpoint() {
    return System.getenv("SPANNER_OMNI_ENDPOINT");
  }

  public static boolean isOmni() {
    return !Strings.isNullOrEmpty(getOmniEndpoint());
  }

  public static String getOmniClientCert() {
    return System.getenv("SPANNER_OMNI_CLIENT_CERT");
  }

  public static String getOmniClientKey() {
    return System.getenv("SPANNER_OMNI_CLIENT_KEY");
  }

  public static boolean isOmniUsePlainText() {
    return Boolean.parseBoolean(System.getenv("SPANNER_OMNI_USE_PLAIN_TEXT"));
  }

  public static String getProject(PipelineOptions options, String instanceProjectId) {
    if (isOmni()) {
      return "default";
    }
    String project = instanceProjectId;
    if (project == null) {
      project = options.as(GcpOptions.class).getProject();
    }
    return project;
  }

  public static String getInstanceId(String instanceId) {
    if (isOmni()) {
      return "default";
    }
    return instanceId;
  }

  public static SpannerOptions.Builder setUpSpannerOptions(SpannerOptions.Builder builder) {
    if (isOmni()) {
      builder.setExperimentalHost(getOmniEndpoint());
      if (isOmniUsePlainText()) {
        builder.usePlainText();
      }
      String cert = getOmniClientCert();
      String key = getOmniClientKey();
      if (!Strings.isNullOrEmpty(cert) && !Strings.isNullOrEmpty(key)) {
        builder.useClientCert(cert, key);
      }
    }
    return builder;
  }

  public static SpannerConfig setUpSpannerConfig(SpannerConfig config) {
    if (isOmni()) {
      config = config.withExperimentalHost(getOmniEndpoint());
      if (isOmniUsePlainText()) {
        config = config.withUsingPlainTextChannel(true);
      }
      String cert = getOmniClientCert();
      String key = getOmniClientKey();
      if (!Strings.isNullOrEmpty(cert) && !Strings.isNullOrEmpty(key)) {
        config = config.withClientCert(cert, key);
      }
    }
    return config;
  }

  public static SpannerIO.Write setUpSpannerIO(SpannerIO.Write write) {
    if (isOmni()) {
      write = write.withExperimentalHost(getOmniEndpoint());
      if (isOmniUsePlainText()) {
        write = write.withUsingPlainTextChannel(true);
      }
      String cert = getOmniClientCert();
      String key = getOmniClientKey();
      if (!Strings.isNullOrEmpty(cert) && !Strings.isNullOrEmpty(key)) {
        write = write.withClientCert(cert, key);
      }
    }
    return write;
  }
}
