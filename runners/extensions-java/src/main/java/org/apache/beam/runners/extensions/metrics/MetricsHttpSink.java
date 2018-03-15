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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.xml.ws.http.HTTPException;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.PipelineOptions;

/** HTTP Sink to push metrics in a POST HTTP request. */
public class MetricsHttpSink implements MetricsSink<String> {
  private final String urlString;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public MetricsHttpSink(PipelineOptions pipelineOptions) {
      this.urlString = pipelineOptions.getMetricsHttpSinkUrl();
  }

  @Override public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
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
    if (responseCode != 200){
      throw new HTTPException(responseCode);
    }
  }
  @VisibleForTesting
  String serializeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    SimpleModule module = new SimpleModule();
    module.addSerializer(MetricQueryResults.class, new MetricQueryResultsSerializer());
    objectMapper.registerModule(module);
    return objectMapper.writeValueAsString(metricQueryResults);
  }
  private static class MetricQueryResultsSerializer extends StdSerializer<MetricQueryResults> {

    public MetricQueryResultsSerializer() {
      super(MetricQueryResults.class);
    }

    @Override
    public void serialize(
        MetricQueryResults metricQueryResults,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeStartObject();

      jsonGenerator.writeArrayFieldStart("counters");
      for (MetricResult<Long> result : metricQueryResults.counters()){
        jsonGenerator.writeStartObject();
        String name = result.name().namespace() + "/"  + result.name().name();
        jsonGenerator.writeStringField("name", name);
        String step = result.step();
        jsonGenerator.writeStringField("step", step);
        Long attempted = result.attempted();
        jsonGenerator.writeNumberField("attempted", attempted);
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();

      jsonGenerator.writeArrayFieldStart("distributions");
      for (MetricResult<DistributionResult> result : metricQueryResults.distributions()){
        jsonGenerator.writeStartObject();
        String name = result.name().namespace() + "/"  + result.name().name();
        jsonGenerator.writeStringField("name", name);
        String step = result.step();
        jsonGenerator.writeStringField("step", step);
        DistributionResult attempted = result.attempted();
        jsonGenerator.writeFieldName("attempted");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("min", attempted.min());
        jsonGenerator.writeNumberField("max", attempted.max());
        jsonGenerator.writeNumberField("sum", attempted.sum());
        jsonGenerator.writeNumberField("count", attempted.count());
        jsonGenerator.writeNumberField("mean", attempted.mean());
        jsonGenerator.writeEndObject();

        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();

      jsonGenerator.writeArrayFieldStart("gauges");
      for (MetricResult<GaugeResult> result : metricQueryResults.gauges()){
        jsonGenerator.writeStartObject();
        String name = result.name().namespace() + "/"  + result.name().name();
        jsonGenerator.writeStringField("name", name);
        String step = result.step();
        jsonGenerator.writeStringField("step", step);
        GaugeResult attempted = result.attempted();
        jsonGenerator.writeFieldName("attempted");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("value", attempted.value());
        jsonGenerator.writeStringField("timestamp", attempted.timestamp().toString());
        jsonGenerator.writeEndObject();
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();

      jsonGenerator.writeEndObject();
    }
  }

}
