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
package org.apache.beam.examples.complete.datatokenization.transforms.io;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.examples.complete.datatokenization.options.DataTokenizationOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The {@link TokenizationBigTableIO} class for writing data from template to BigTable. */
public class TokenizationBigTableIO {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(TokenizationBigTableIO.class);

  private final DataTokenizationOptions options;

  public TokenizationBigTableIO(DataTokenizationOptions options) {
    this.options = options;
  }

  public PDone write(PCollection<Row> input, Schema schema) {
    return input
        .apply("ConvertToBigTableFormat", ParDo.of(new TransformToBigTableFormat(schema)))
        .apply(
            "WriteToBigTable",
            BigtableIO.write()
                .withProjectId(options.getBigTableProjectId())
                .withInstanceId(options.getBigTableInstanceId())
                .withTableId(options.getBigTableTableId())
                .withWriteResults())
        .apply("LogRowCount", new LogSuccessfulRows());
  }

  static class TransformToBigTableFormat extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

    private final Schema schema;

    TransformToBigTableFormat(Schema schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(
        @Element Row in, OutputReceiver<KV<ByteString, Iterable<Mutation>>> out, ProcessContext c) {
      DataTokenizationOptions options = c.getPipelineOptions().as(DataTokenizationOptions.class);
      // Mapping every field in provided Row to Mutation.SetCell, which will create/update
      // cell content with provided data
      Set<Mutation> mutations =
          schema.getFields().stream()
              .map(Schema.Field::getName)
              // Ignoring key field, otherwise it will be added as regular column
              .filter(fieldName -> !Objects.equals(fieldName, options.getBigTableKeyColumnName()))
              .map(fieldName -> Pair.of(fieldName, in.getString(fieldName)))
              .map(
                  pair ->
                      Mutation.newBuilder()
                          .setSetCell(
                              Mutation.SetCell.newBuilder()
                                  .setFamilyName(options.getBigTableColumnFamilyName())
                                  .setColumnQualifier(
                                      ByteString.copyFrom(pair.getKey(), StandardCharsets.UTF_8))
                                  .setValue(
                                      ByteString.copyFrom(pair.getValue(), StandardCharsets.UTF_8))
                                  .setTimestampMicros(System.currentTimeMillis() * 1000)
                                  .build())
                          .build())
              .collect(Collectors.toSet());
      // Converting key value to BigTable format
      String columnName = in.getString(options.getBigTableKeyColumnName());
      if (columnName != null) {
        ByteString key = ByteString.copyFrom(columnName, StandardCharsets.UTF_8);
        out.output(KV.of(key, mutations));
      }
    }
  }

  static class LogSuccessfulRows extends PTransform<PCollection<BigtableWriteResult>, PDone> {

    @Override
    public PDone expand(PCollection<BigtableWriteResult> input) {
      input.apply(
          ParDo.of(
              new DoFn<BigtableWriteResult, Void>() {
                @ProcessElement
                public void processElement(@Element BigtableWriteResult in) {
                  LOG.info("Successfully wrote {} rows.", in.getRowsWritten());
                }
              }));
      return PDone.in(input.getPipeline());
    }
  }

  /**
   * Necessary {@link PipelineOptions} options for Pipelines that perform write operations to
   * BigTable.
   */
  public interface BigTableOptions extends PipelineOptions {

    @Description("Id of the project where the Cloud BigTable instance to write into is located.")
    String getBigTableProjectId();

    void setBigTableProjectId(String bigTableProjectId);

    @Description("Id of the Cloud BigTable instance to write into.")
    String getBigTableInstanceId();

    void setBigTableInstanceId(String bigTableInstanceId);

    @Description("Id of the Cloud BigTable table to write into.")
    String getBigTableTableId();

    void setBigTableTableId(String bigTableTableId);

    @Description("Column name to use as a key in Cloud BigTable.")
    String getBigTableKeyColumnName();

    void setBigTableKeyColumnName(String bigTableKeyColumnName);

    @Description("Column family name to use in Cloud BigTable.")
    String getBigTableColumnFamilyName();

    void setBigTableColumnFamilyName(String bigTableColumnFamilyName);
  }
}
