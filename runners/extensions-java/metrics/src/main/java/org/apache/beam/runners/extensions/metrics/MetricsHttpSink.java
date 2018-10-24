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
package org.apache.beam.runners.extensions.metrics;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.xml.ws.http.HTTPException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.PipelineOptions;

/** HTTP Sink to push metrics in a POST HTTP request. */
public class MetricsHttpSink implements MetricsSink {
  private final String urlString;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public MetricsHttpSink(PipelineOptions pipelineOptions) {
    this.urlString = pipelineOptions.getMetricsHttpSinkUrl();
  }

  @Experimental(Experimental.Kind.METRICS)
  @Override
  public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    URL url = new URL(urlString);
    String metrics = serializeMetrics(metricQueryResults);
    byte[] postData = metrics.getBytes(StandardCharsets.UTF_8);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoOutput(true);
    connection.setInstanceFollowRedirects(false);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("charset", "utf-8");
    connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
    connection.setUseCaches(false);
    try (DataOutputStream connectionOuputStream =
        new DataOutputStream(connection.getOutputStream())) {
      connectionOuputStream.write(postData);
    }
    int responseCode = connection.getResponseCode();
    if (responseCode != 200) {
      throw new HTTPException(responseCode);
    }
  }

  private String serializeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    objectMapper.registerModule(new JodaModule());
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    // need to register a filter as soon as @JsonFilter annotation is specified.
    // So specify an pass through filter
    SimpleBeanPropertyFilter filter = SimpleBeanPropertyFilter.serializeAll();
    SimpleFilterProvider filterProvider = new SimpleFilterProvider();
    filterProvider.addFilter("committedMetrics", filter);
    objectMapper.setFilterProvider(filterProvider);
    String result;
    try {
      result = objectMapper.writeValueAsString(metricQueryResults);
    } catch (JsonMappingException exception) {
      if ((exception.getCause() instanceof UnsupportedOperationException)
          && exception.getCause().getMessage().contains("committed metrics")) {
        filterProvider.removeFilter("committedMetrics");
        filter = SimpleBeanPropertyFilter.serializeAllExcept("committed");
        filterProvider.addFilter("committedMetrics", filter);
        result = objectMapper.writeValueAsString(metricQueryResults);
      } else {
        throw exception;
      }
    }
    return result;
  }
}
