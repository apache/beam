package org.apache.beam.sdk.extensions.tpc;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.csvLines2BeamRows;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;

/** Read-side converter for {@link TextTable} with format {@code 'csv'}. */
public class CsvToRow extends PTransform<PCollection<String>, PCollection<Row>>
    implements Serializable {

  private Schema schema;
  private CSVFormat csvFormat;

  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  public CsvToRow(Schema schema, CSVFormat csvFormat) {
    this.schema = schema;
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<Row> expand(PCollection<String> input) {
    return input.apply(
        "csvToRow",
        FlatMapElements.into(TypeDescriptors.rows())
            .via(s -> csvLines2BeamRows(csvFormat, s, schema)));
  }
}
