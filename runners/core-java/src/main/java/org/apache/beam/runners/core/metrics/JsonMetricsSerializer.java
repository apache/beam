package org.apache.beam.runners.core.metrics;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.beam.sdk.metrics.MetricQueryResults;

/** Serialize metrics into json representation to be pushed to a backend. */
public class JsonMetricsSerializer implements MetricsSerializer<String> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String serializeMetrics(MetricQueryResults metricResults) throws Exception {
    SimpleModule module = new SimpleModule();
    module.addSerializer(MetricQueryResults.class, new MetricQueryResultsSerializer());
    objectMapper.registerModule(module);
    return objectMapper.writeValueAsString(metricResults);
  }

  private class MetricQueryResultsSerializer extends StdSerializer<MetricQueryResults> {

    public MetricQueryResultsSerializer() {
      this(null);
    }

    public MetricQueryResultsSerializer(Class<MetricQueryResults> t) {
      super(t);
    }

    @Override
    public void serialize(
        MetricQueryResults metricQueryResults,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      // TODO replace dummy serializer
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("dummy", "toto");
      jsonGenerator.writeEndObject();
    }
  }
}
