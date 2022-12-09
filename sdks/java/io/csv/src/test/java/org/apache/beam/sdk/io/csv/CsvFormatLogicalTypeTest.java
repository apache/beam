package org.apache.beam.sdk.io.csv;

import static org.junit.Assert.assertEquals;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Predefined;
import org.junit.Test;

/** Tests for {@link CsvFormatLogicalType}. */
public class CsvFormatLogicalTypeTest {

  @Test
  public void toBaseType() {
    CsvFormatLogicalType logicalType = new CsvFormatLogicalType();
    for (Predefined predefined : Predefined.values()) {
      CSVFormat input = CSVFormat.valueOf(predefined.name());
      assertEquals(predefined.name(), logicalType.toBaseType(input));
    }
  }

  @Test
  public void toInputType() {
    CsvFormatLogicalType logicalType = new CsvFormatLogicalType();
    for (Predefined predefined : Predefined.values()) {
      String input = predefined.name();
      CSVFormat expect = CSVFormat.valueOf(input);
      assertEquals(expect, logicalType.toInputType(input));
    }
  }
}