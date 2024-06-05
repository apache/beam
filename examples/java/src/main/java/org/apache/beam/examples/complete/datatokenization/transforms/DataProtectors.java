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
package org.apache.beam.examples.complete.datatokenization.transforms;

import static org.apache.beam.sdk.util.RowJsonUtils.rowToJson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElement;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElementCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonArray;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonObject;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataProtectors} Using passing parameters transform will buffer input rows in batch and
 * will send it when the count of buffered rows will equal specified batch size. When it takes the
 * last one batch, it will send it when the last row will come to doFn even count of buffered rows
 * will less than the batch size.
 */
public class DataProtectors {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(DataProtectors.class);

  public static final String ID_FIELD_NAME = "ID";
  private static final Long MAX_BUFFERING_DURATION_MS =
      Long.valueOf(System.getenv().getOrDefault("MAX_BUFFERING_DURATION_MS", "100"));

  /**
   * The {@link RowToTokenizedRow} transform converts {@link Row} to {@link TableRow} objects. The
   * transform accepts a {@link FailsafeElement} object so the original payload of the incoming
   * record can be maintained across multiple series of transforms.
   */
  @AutoValue
  public abstract static class RowToTokenizedRow<T>
      extends PTransform<PCollection<KV<Integer, Row>>, PCollectionTuple> {

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_DataProtectors_RowToTokenizedRow.Builder<>();
    }

    public abstract TupleTag<Row> successTag();

    public abstract TupleTag<FailsafeElement<Row, Row>> failureTag();

    public abstract Schema schema();

    public abstract int batchSize();

    public abstract String rpcURI();

    @Override
    public PCollectionTuple expand(PCollection<KV<Integer, Row>> inputRows) {
      FailsafeElementCoder<Row, Row> coder =
          FailsafeElementCoder.of(RowCoder.of(schema()), RowCoder.of(schema()));

      Duration maxBuffering = Duration.millis(MAX_BUFFERING_DURATION_MS);
      PCollectionTuple pCollectionTuple =
          inputRows
              .apply(
                  "GroupRowsIntoBatches",
                  GroupIntoBatches.<Integer, Row>ofSize(batchSize())
                      .withMaxBufferingDuration(maxBuffering))
              .apply(
                  "Tokenize",
                  ParDo.of(new TokenizationFn(schema(), rpcURI(), failureTag()))
                      .withOutputTags(successTag(), TupleTagList.of(failureTag())));

      return PCollectionTuple.of(
              successTag(), pCollectionTuple.get(successTag()).setRowSchema(schema()))
          .and(failureTag(), pCollectionTuple.get(failureTag()).setCoder(coder));
    }

    /** Builder for {@link RowToTokenizedRow}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setSuccessTag(TupleTag<Row> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<Row, Row>> failureTag);

      public abstract Builder<T> setSchema(Schema schema);

      public abstract Builder<T> setBatchSize(int batchSize);

      public abstract Builder<T> setRpcURI(String rpcURI);

      public abstract RowToTokenizedRow<T> build();
    }
  }

  /** Class implements stateful doFn for data tokenization using remote RPC. */
  @SuppressWarnings({
    "initialization.static.fields.uninitialized",
    "initialization.static.field.uninitialized"
  })
  public static class TokenizationFn extends DoFn<KV<Integer, Iterable<Row>>, Row> {

    private static Schema schemaToRpc;
    private static CloseableHttpClient httpclient;
    private static ObjectMapper objectMapperSerializerForSchema;
    private static ObjectMapper objectMapperDeserializerForSchema;

    private final Schema schema;
    private final String rpcURI;
    private final TupleTag<FailsafeElement<Row, Row>> failureTag;

    private Map<String, Row> inputRowsWithIds;

    public TokenizationFn(
        Schema schema, String rpcURI, TupleTag<FailsafeElement<Row, Row>> failureTag) {
      this.schema = schema;
      this.rpcURI = rpcURI;
      this.failureTag = failureTag;
      this.inputRowsWithIds = new HashMap<>();
    }

    @Setup
    public void setup() {

      List<Field> fields = schema.getFields();
      fields.add(Field.of(ID_FIELD_NAME, FieldType.STRING));
      schemaToRpc = new Schema(fields);

      objectMapperSerializerForSchema =
          RowJsonUtils.newObjectMapperWith(RowJson.RowJsonSerializer.forSchema(schemaToRpc));

      objectMapperDeserializerForSchema =
          RowJsonUtils.newObjectMapperWith(RowJson.RowJsonDeserializer.forSchema(schemaToRpc));

      httpclient = HttpClients.createDefault();
    }

    @Teardown
    public void close() {
      try {
        httpclient.close();
      } catch (IOException exception) {
        String exceptionMessage = exception.getMessage();
        if (exceptionMessage != null) {
          LOG.warn("Can't close connection: {}", exceptionMessage);
        }
      }
    }

    @ProcessElement
    @SuppressWarnings("argument")
    public void process(@Element KV<Integer, Iterable<Row>> element, ProcessContext context) {
      Iterable<Row> rows = element.getValue();

      try {
        for (Row outputRow : getTokenizedRow(rows)) {
          context.output(outputRow);
        }
      } catch (Exception e) {
        for (Row outputRow : rows) {
          context.output(
              failureTag,
              FailsafeElement.of(outputRow, outputRow)
                  .setErrorMessage(e.getMessage())
                  .setStacktrace(Throwables.getStackTraceAsString(e)));
        }
      }
    }

    private ArrayList<String> rowsToJsons(Iterable<Row> inputRows) {
      ArrayList<String> jsons = new ArrayList<>();
      Map<String, Row> inputRowsWithIds = new HashMap<>();
      for (Row inputRow : inputRows) {

        Row.Builder builder = Row.withSchema(schemaToRpc);
        for (Schema.Field field : schemaToRpc.getFields()) {
          if (inputRow.getSchema().hasField(field.getName())) {
            builder = builder.addValue(inputRow.getValue(field.getName()));
          }
        }
        String id = UUID.randomUUID().toString();
        builder = builder.addValue(id);
        inputRowsWithIds.put(id, inputRow);

        Row row = builder.build();

        jsons.add(rowToJson(objectMapperSerializerForSchema, row));
      }
      this.inputRowsWithIds = inputRowsWithIds;
      return jsons;
    }

    private String formatJsonsToRpcBatch(Iterable<String> jsons) {
      StringBuilder stringBuilder = new StringBuilder(String.join(",", jsons));
      stringBuilder.append("]").insert(0, "{\"data\": [").append("}");
      return stringBuilder.toString();
    }

    @SuppressWarnings("argument")
    private ArrayList<Row> getTokenizedRow(Iterable<Row> inputRows) throws IOException {
      ArrayList<Row> outputRows = new ArrayList<>();

      CloseableHttpResponse response =
          sendRpc(formatJsonsToRpcBatch(rowsToJsons(inputRows)).getBytes(Charset.defaultCharset()));

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        LOG.error("Send to RPC '{}' failed with '{}'", this.rpcURI, response.getStatusLine());
      }

      String tokenizedData =
          IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);

      Gson gson = new Gson();
      JsonArray jsonTokenizedRows =
          gson.fromJson(tokenizedData, JsonObject.class).getAsJsonArray("data");

      for (int i = 0; i < jsonTokenizedRows.size(); i++) {
        Row tokenizedRow =
            RowJsonUtils.jsonToRow(
                objectMapperDeserializerForSchema, jsonTokenizedRows.get(i).toString());
        Row.FieldValueBuilder rowBuilder =
            Row.fromRow(this.inputRowsWithIds.get(tokenizedRow.getString(ID_FIELD_NAME)));
        for (Schema.Field field : schemaToRpc.getFields()) {
          if (field.getName().equals(ID_FIELD_NAME)) {
            continue;
          }
          rowBuilder =
              rowBuilder.withFieldValue(field.getName(), tokenizedRow.getValue(field.getName()));
        }
        outputRows.add(rowBuilder.build());
      }

      return outputRows;
    }

    private CloseableHttpResponse sendRpc(byte[] data) throws IOException {
      HttpPost httpPost = new HttpPost(rpcURI);
      HttpEntity stringEntity = new ByteArrayEntity(data, ContentType.APPLICATION_JSON);
      httpPost.setEntity(stringEntity);
      return httpclient.execute(httpPost);
    }
  }
}
