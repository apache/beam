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
import java.nio.charset.Charset;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

/** */
public class GceMetadataUtil {
  private static final String BASE_METADATA_URL = "http://metadata/computeMetadata/v1/";

  static String fetchMetadata(String key) {
    int timeoutMillis = 5000;
    final HttpParams httpParams = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(httpParams, timeoutMillis);
    HttpClient client = new DefaultHttpClient(httpParams);
    HttpGet request = new HttpGet(BASE_METADATA_URL + key);
    request.setHeader("Metadata-Flavor", "Google");

    try {
      HttpResponse response = client.execute(request);
      if (response.getStatusLine().getStatusCode() != 200) {
        // May mean its running on a non DataflowRunner, in which case it's perfectly normal.
        return "";
      }
      InputStream in = response.getEntity().getContent();
      try (final Reader reader = new InputStreamReader(in, Charset.defaultCharset())) {
        return CharStreams.toString(reader);
      }
    } catch (IOException e) {
      // May mean its running on a non DataflowRunner, in which case it's perfectly normal.
    }
    return "";
  }

  private static String fetchCustomGceMetadata(String customMetadataKey) {
    return GceMetadataUtil.fetchMetadata("instance/attributes/" + customMetadataKey);
  }

  public static String fetchDataflowJobId() {
    return GceMetadataUtil.fetchCustomGceMetadata("job_id");
  }
}
