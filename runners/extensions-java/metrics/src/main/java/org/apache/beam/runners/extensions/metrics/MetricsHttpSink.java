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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.metrics.MetricsSink;

/** HTTP Sink to push metrics in a POST HTTP request. */
public class MetricsHttpSink implements MetricsSink {
  private final String urlString;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public MetricsHttpSink(MetricsOptions pipelineOptions) {
    this.urlString = pipelineOptions.getMetricsHttpSinkUrl();
  }

  /**
   * Writes the metricQueryResults via HTTP POST to metrics sink endpoint.
   *
   * @param metricQueryResults query results to write.
   * @throws Exception throws IOException for non-200 response from endpoint.
   */
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
      throw new IOException(
          "Expected HTTP 200 OK response while writing metrics to MetricsHttpSink but received "
              + responseCode);
    }
  }

  /**
   * JSON serializer for {@link MetricName}; simple {namespace,name} for user-metrics, full URN for
   * system metrics.
   */
  public static class MetricNameSerializer extends StdSerializer<MetricName> {
    public MetricNameSerializer(Class<MetricName> t) {
      super(t);
    }

    @Override
    public void serialize(MetricName value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeStartObject();
      gen.writeObjectField("name", value.getName());
      gen.writeObjectField("namespace", value.getNamespace());
      gen.writeEndObject();
    }
  }

  /**
   * JSON serializer for {@link MetricKey}; output a {@link MetricName "name"} object and a "step"
   * or "pcollection" field with the corresponding label.
   */
  public static class MetricKeySerializer extends StdSerializer<MetricKey> {
    public MetricKeySerializer(Class<MetricKey> t) {
      super(t);
    }

    public void inline(MetricKey value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeObjectField("name", value.metricName());
      gen.writeObjectField("step", value.stepName());
    }

    @Override
    public void serialize(MetricKey value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeStartObject();
      inline(value, gen, provider);
      gen.writeEndObject();
    }
  }

  /**
   * JSON serializer for {@link MetricResult}; conform to an older format where the {@link MetricKey
   * key's} {@link MetricName name} and "step" (ptransform) are inlined.
   */
  public static class MetricResultSerializer extends StdSerializer<MetricResult> {
    private final MetricKeySerializer keySerializer;

    public MetricResultSerializer(Class<MetricResult> t) {
      super(t);
      keySerializer = new MetricKeySerializer(MetricKey.class);
    }

    @Override
    public void serialize(MetricResult value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeStartObject();
      gen.writeObjectField("attempted", value.getAttempted());
      if (value.hasCommitted()) {
        gen.writeObjectField("committed", value.getCommitted());
      }
      keySerializer.inline(value.getKey(), gen, provider);
      gen.writeEndObject();
    }
  }

  private String serializeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    SimpleModule module = new JodaModule();
    module.addSerializer(new MetricNameSerializer(MetricName.class));
    module.addSerializer(new MetricKeySerializer(MetricKey.class));
    module.addSerializer(new MetricResultSerializer(MetricResult.class));
    objectMapper.registerModule(module);
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
