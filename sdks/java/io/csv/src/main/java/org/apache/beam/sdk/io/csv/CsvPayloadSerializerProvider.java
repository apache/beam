package org.apache.beam.sdk.io.csv;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.commons.csv.CSVFormat;

@AutoService(PayloadSerializerProvider.class)
public class CsvPayloadSerializerProvider implements PayloadSerializerProvider {
  @Override
  public String identifier() {
    return "csv";
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> params) {
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    for (Object value : params.values()) {
      if (value instanceof CSVFormat) {
        csvFormat = (CSVFormat) value;
      }
    }
    return new CsvPayloadSerializer(csvFormat, schema);
  }
}
