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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery write jobs configured
 * using {@link BigQuerySchemaTransformConfiguration.Write}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
@Internal
@Experimental(Kind.SCHEMAS)
public class BigQuerySchemaTransformWriteProvider
    extends TypedSchemaTransformProvider<BigQuerySchemaTransformConfiguration.Write> {

  private static final String API = "bigquery";
  private static final String VERSION = "v2";
  private static final String INPUT_TAG = "INPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<BigQuerySchemaTransformConfiguration.Write> configurationClass() {
    return BigQuerySchemaTransformConfiguration.Write.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(BigQuerySchemaTransformConfiguration.Write configuration) {
    return new BigQueryWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return String.format("%s:%s", API, VERSION);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since a
   * single is expected, this returns a list with a single name.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. Since
   * no output is expected, this returns an empty list.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * A {@link SchemaTransform} that performs {@link BigQueryIO.Write}s based on a {@link
   * BigQuerySchemaTransformConfiguration.Write}.
   */
  static class BigQueryWriteSchemaTransform implements SchemaTransform {
    private final BigQuerySchemaTransformConfiguration.Write configuration;

    BigQueryWriteSchemaTransform(BigQuerySchemaTransformConfiguration.Write configuration) {
      this.configuration = configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PCollectionRowTupleTransform(configuration);
    }
  }

  /**
   * An implementation of {@link PTransform} for BigQuery write jobs configured using {@link
   * BigQuerySchemaTransformConfiguration.Write}.
   */
  static class PCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {

    private final BigQuerySchemaTransformConfiguration.Write configuration;

    /** An instance of {@link BigQueryServices} used for testing. */
    private BigQueryServices testBigQueryServices = null;

    PCollectionRowTupleTransform(BigQuerySchemaTransformConfiguration.Write configuration) {
      this.configuration = configuration;
    }

    @Override
    public void validate(PipelineOptions options) {
      CreateDisposition createDisposition = configuration.getCreateDispositionEnum();
      Schema destinationSchema = getDestinationRowSchema(options);

      if (destinationSchema == null) {
        // We only care if the create disposition implies an existing table i.e. create never.
        if (createDisposition.equals(CreateDisposition.CREATE_NEVER)) {
          throw new InvalidConfigurationException(
              String.format(
                  "configuration create disposition: %s for table: %s for a null destination schema",
                  createDisposition, configuration.getTableSpec()));
        }
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      validate(input);
      PCollection<Row> rowPCollection = input.get(INPUT_TAG);
      Schema schema = rowPCollection.getSchema();
      BigQueryIO.Write<TableRow> write = toWrite(schema);
      if (testBigQueryServices != null) {
        write = write.withTestServices(testBigQueryServices);
      }

      PCollection<TableRow> tableRowPCollection =
          rowPCollection.apply(
              MapElements.into(TypeDescriptor.of(TableRow.class)).via(BigQueryUtils::toTableRow));
      tableRowPCollection.apply(write);
      return PCollectionRowTuple.empty(input.getPipeline());
    }

    /** Instantiates a {@link BigQueryIO.Write<TableRow>} from a {@link Schema}. */
    BigQueryIO.Write<TableRow> toWrite(Schema schema) {
      TableSchema tableSchema = BigQueryUtils.toTableSchema(schema);
      CreateDisposition createDisposition =
          CreateDisposition.valueOf(configuration.getCreateDisposition());
      WriteDisposition writeDisposition =
          WriteDisposition.valueOf(configuration.getWriteDisposition());

      return BigQueryIO.writeTableRows()
          .to(configuration.getTableSpec())
          .withCreateDisposition(createDisposition)
          .withWriteDisposition(writeDisposition)
          .withSchema(tableSchema);
    }

    /** Setter for testing using {@link BigQueryServices}. */
    void setTestBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }

    /** Validate a {@link PCollectionRowTuple} input. */
    void validate(PCollectionRowTuple input) {
      if (!input.has(INPUT_TAG)) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s is missing expected tag: %s",
                getClass().getSimpleName(), input.getClass().getSimpleName(), INPUT_TAG));
      }

      validate(input.get(INPUT_TAG));
    }

    /** Validate a {@link PCollection<Row>} input. */
    void validate(PCollection<Row> input) {
      Schema sourceSchema = input.getSchema();
      if (sourceSchema == null) {
        throw new IllegalArgumentException(
            String.format("%s is null for input of tag: %s", Schema.class, INPUT_TAG));
      }

      Schema destinationSchema = getDestinationRowSchema(input.getPipeline().getOptions());

      // We already evaluate whether we should acquire a destination schema.
      // See the validate(PipelineOptions) method for details.
      if (destinationSchema != null) {
        validateMatching(sourceSchema, destinationSchema);
      }
    }

    void validateMatching(Schema sourceSchema, Schema destinationSchema) {
      Set<String> fieldNames = new HashSet<>();
      List<String> mismatchedFieldNames = new ArrayList<>();
      fieldNames.addAll(sourceSchema.getFieldNames());
      fieldNames.addAll(destinationSchema.getFieldNames());
      for (String name : fieldNames) {
        Field gotField = null;
        Field wantField = null;
        if (sourceSchema.hasField(name)) {
          gotField = sourceSchema.getField(name);
        }
        if (destinationSchema.hasField(name)) {
          wantField = destinationSchema.getField(name);
        }
        if (!matches(wantField, gotField)) {
          mismatchedFieldNames.add(name);
        }
      }

      if (!mismatchedFieldNames.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "source and destination schema mismatch for table: %s with fields: %s",
                configuration.getTableSpec(), String.join(" | ", mismatchedFieldNames)));
      }
    }

    private boolean matches(Field want, Field got) {
      if (want == null && got == null) {
        return true;
      }
      if (want == null) {
        return false;
      }
      return want.equals(got);
    }

    TableSchema getDestinationTableSchema(BigQueryOptions options) {
      Table destinationTable = getDestinationTable(options);
      if (destinationTable == null) {
        return null;
      }
      return destinationTable.getSchema();
    }

    DatasetService getDatasetService(BigQueryOptions options) {
      BigQueryServices bigQueryServices = testBigQueryServices;
      if (bigQueryServices == null) {
        bigQueryServices = new BigQueryServicesImpl();
      }
      return bigQueryServices.getDatasetService(options);
    }

    Table getDestinationTable(BigQueryOptions options) {
      Table destinationTable = null;
      CreateDisposition createDisposition = configuration.getCreateDispositionEnum();
      DatasetService datasetService = getDatasetService(options);
      try {
        destinationTable = datasetService.getTable(configuration.getTableReference());
      } catch (IOException | InterruptedException e) {
        // We only care if the create disposition implies an existing table i.e. create never.
        if (createDisposition.equals(CreateDisposition.CREATE_NEVER)) {
          throw new InvalidConfigurationException(
              String.format(
                  "error querying destination schema for create disposition: %s for table: %s, error: %s",
                  createDisposition, configuration.getTableSpec(), e.getMessage()));
        }
      }
      return destinationTable;
    }

    Schema getDestinationRowSchema(BigQueryOptions options) {
      TableSchema tableSchema = getDestinationTableSchema(options);
      if (tableSchema == null) {
        return null;
      }
      return BigQueryUtils.fromTableSchema(tableSchema);
    }

    Schema getDestinationRowSchema(PipelineOptions options) {
      return getDestinationRowSchema(options.as(BigQueryOptions.class));
    }
  }
}
