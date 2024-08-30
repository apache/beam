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
package org.apache.beam.sdk.extensions.gcp.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CharStreams;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class GceMetadataUtil {
  private static final String BASE_METADATA_URL = "http://metadata/computeMetadata/v1/";

  private static final Logger LOG = LoggerFactory.getLogger(GceMetadataUtil.class);

  static String fetchMetadata(String key) {
    String requestUrl = BASE_METADATA_URL + key;
    int timeoutMillis = 5000;
    final HttpParams httpParams = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(httpParams, timeoutMillis);
    HttpConnectionParams.setSoTimeout(httpParams, timeoutMillis);
    String ret = "";
    try {
      HttpClient client = new DefaultHttpClient(httpParams);

      HttpGet request = new HttpGet(requestUrl);
      request.setHeader("Metadata-Flavor", "Google");

      HttpResponse response = client.execute(request);
      if (response.getStatusLine().getStatusCode() == 200) {
        InputStream in = response.getEntity().getContent();
        try (final Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
          ret = CharStreams.toString(reader);
        }
      }
    } catch (IOException ignored) {
    }

    // The return value can be an empty string, which may mean it's running on a non DataflowRunner.
    LOG.debug("Fetched GCE Metadata at '{}' and got '{}'", requestUrl, ret);

    return ret;
  }

  private static String fetchVmInstanceMetadata(String instanceMetadataKey) {
    return GceMetadataUtil.fetchMetadata("instance/" + instanceMetadataKey);
  }

  private static String fetchCustomGceMetadata(String customMetadataKey) {
    return GceMetadataUtil.fetchVmInstanceMetadata("attributes/" + customMetadataKey);
  }

  public static String fetchDataflowJobId() {
    return GceMetadataUtil.fetchCustomGceMetadata("job_id");
  }

  public static String fetchDataflowJobName() {
    return GceMetadataUtil.fetchCustomGceMetadata("job_name");
  }

  public static String fetchDataflowWorkerId() {
    return GceMetadataUtil.fetchVmInstanceMetadata("id");
  }
}
