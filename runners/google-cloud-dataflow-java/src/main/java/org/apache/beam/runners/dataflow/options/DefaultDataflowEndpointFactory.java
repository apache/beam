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
package org.apache.beam.runners.dataflow.options;

import com.google.api.services.dataflow.Dataflow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for a default value for Dataflow endpoint calculated based on the region option.
 */
@VisibleForTesting
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DefaultDataflowEndpointFactory implements DefaultValueFactory<String> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultGcpRegionFactory.class);

  @Override
  public String create(PipelineOptions options) {
    String region = options.getRegion();
    if (!Strings.isNullOrEmpty(region)) {
      LOG.info("Region is set. Using Region to determine endpoint.", region);
      return calculateDataflowEndpointFromRegion(region);
    }
    else {
      return Dataflow.DEFAULT_SERVICE_PATH;
    }

  }

  @VisibleForTesting
  public static String calculateDataflowEndpointFromRegion(String region) {
    String baseUrl = Dataflow.DEFAULT_SERVICE_PATH;
    return region + "-" + baseUrl;
  }

}
