package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Convenience transform to write dead-letter {@link HealthcareIOError}s to BigQuery
 * {@link TableRow}s.
 *
 * @param <T> the type parameter
 */
public class HealthcareIOErrorToTableRow<T>
    extends PTransform<PCollection<HealthcareIOError<T>>, PCollection<TableRow>> {

  private static final DateTimeFormatter DATETIME_FORMATTER = ISODateTimeFormat.dateTime();

  @Override
  public PCollection<TableRow> expand(PCollection<HealthcareIOError<T>> input) {
    return input.apply(
        MapElements
            .into(TypeDescriptor.of(TableRow.class))
            .via((HealthcareIOError<T> err) ->{
              TableRow out = new TableRow();
              out.set("data_element", err.getDataResource().toString());
              out.set("observed_time", err.getObservedTime().toString(DATETIME_FORMATTER));
              out.set("message", err.getErrorMessage());
              out.set("stacktrace", err.getStackTrace());
              return out;
            })
    );
  }
}
