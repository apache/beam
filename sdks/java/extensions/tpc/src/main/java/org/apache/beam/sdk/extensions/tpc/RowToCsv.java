package org.apache.beam.sdk.extensions.tpc;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;

/**
 * RowToCsv.
 */
public class RowToCsv extends PTransform<PCollection<Row>, PCollection<String>>
        implements Serializable {

  private CSVFormat csvFormat;

  public RowToCsv(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  @Override
  public PCollection<String> expand(PCollection<Row> input) {
    return input.apply(
            "rowToCsv",
            MapElements.into(TypeDescriptors.strings()).via(row ->
                    beamRow2CsvLine(row, csvFormat)));
  }
}
