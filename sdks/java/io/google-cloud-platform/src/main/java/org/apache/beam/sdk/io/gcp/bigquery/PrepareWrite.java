package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Prepare an input {@link PCollection<T>} for writing to BigQuery. Use the table-reference
 * function to determine which tables each element is written to, and format the element into a
 * {@link TableRow} using the user-supplied format function.
 */
public class PrepareWrite<T> extends PTransform<PCollection<T>, PCollection<KV<String, TableRow>>> {
  private static final String NAME = "PrepareWrite";
  private SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction;
  private SerializableFunction<T, TableRow> formatFunction;

  public PrepareWrite(SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction,
                      SerializableFunction<T, TableRow> formatFunction) {
    super(NAME);
    this.tableRefFunction = tableRefFunction;
    this.formatFunction = formatFunction;
  }

  @Override
  public PCollection<KV<String, TableRow>> expand(PCollection<T> input) {
    PCollection<KV<String, TableRow>> elementsByTable =
        input.apply(ParDo.of(new DoFn<T, KV<String, TableRow>>() {
      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
        String tableSpec = tableSpecFromWindowedValue(
            context.getPipelineOptions().as(BigQueryOptions.class),
            ValueInSingleWindow.of(context.element(), context.timestamp(), window, context.pane()));
        TableRow tableRow = formatFunction.apply(context.element());
        context.output(KV.of(tableSpec, tableRow));
      }
    }));
    return elementsByTable;
  }

  private String tableSpecFromWindowedValue(BigQueryOptions options,
                                            ValueInSingleWindow<T> value) {
    TableReference table = tableRefFunction.apply(value);
    if (Strings.isNullOrEmpty(table.getProjectId())) {
      table.setProjectId(options.getProject());
    }
    return BigQueryHelpers.toTableSpec(table);
  }
}
