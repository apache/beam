package org.apache.beam.sdk.io.csv;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Predefined;

/** {@link Schema.LogicalType} for {@link CSVFormat}. */
@Experimental(Kind.SCHEMAS)
public class CsvFormatLogicalType implements Schema.LogicalType<CSVFormat, String> {
  public static final String IDENTIFIER = "beam:logical_type:csv_format:v1";
  private static final Map<CSVFormat, String> FORMAT_STRING_MAP = new HashMap<>();

  static {
    for (Predefined predefined : Predefined.values()) {
      CSVFormat csvFormat = CSVFormat.valueOf(predefined.name());
      FORMAT_STRING_MAP.put(csvFormat, predefined.name());
    }
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  // unused
  @Override
  public Schema.FieldType getArgumentType() {
    return FieldType.STRING;
  }

  @Override
  public FieldType getBaseType() {
    return FieldType.STRING;
  }

  /** Converts a {@link CSVFormat} to its {@link Predefined#name()}. */
  @Override
  public String toBaseType(CSVFormat input) {
    if (!FORMAT_STRING_MAP.containsKey(input)) {
      throw new IllegalArgumentException(String.format("%s not supported", input));
    }
    return FORMAT_STRING_MAP.get(input);
  }

  /** Converts to a {@link CSVFormat} from its {@link Predefined#name()}. */
  @Override
  public CSVFormat toInputType(String base) {
    return CSVFormat.valueOf(base);
  }
}
