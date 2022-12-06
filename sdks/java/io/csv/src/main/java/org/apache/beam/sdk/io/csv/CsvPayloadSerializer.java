package org.apache.beam.sdk.io.csv;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CsvPayloadSerializer implements PayloadSerializer {

  private final CSVFormat csvFormat;
  private final Schema schema;

  CsvPayloadSerializer(Schema schema, @Nullable CSVFormat csvFormat) {
    this.schema = schema;
    if (csvFormat == null) {
      csvFormat = CSVFormat.DEFAULT;
    }
    this.csvFormat = csvFormat;
  }

  @Override
  public byte[] serialize(Row row) {
    return csvFormat.format(row.getValues()).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Row deserialize(byte[] bytes) {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
