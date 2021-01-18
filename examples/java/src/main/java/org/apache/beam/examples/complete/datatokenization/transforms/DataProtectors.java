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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

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
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonArray;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TODO: Add javadoc. */
public class DataProtectors {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(DataProtectors.class);

  public static final String ID_FIELD_NAME = "ID";

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
      PCollectionTuple pCollectionTuple =
          inputRows.apply(
              "Tokenize",
              ParDo.of(new TokenizationFn(schema(), batchSize(), rpcURI(), failureTag()))
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

  /** Class for data tokenization. */
  @SuppressWarnings("initialization.static.fields.uninitialized")
  public static class TokenizationFn extends DoFn<KV<Integer, Row>, Row> {

    private static Schema schemaToRpc;
    private static CloseableHttpClient httpclient;
    private static ObjectMapper objectMapperSerializerForSchema;
    private static ObjectMapper objectMapperDeserializerForSchema;

    private final Schema schema;
    private final int batchSize;
    private final String rpcURI;
    private final TupleTag<FailsafeElement<Row, Row>> failureTag;

    @StateId("buffer")
    private final StateSpec<BagState<Row>> bufferedEvents;

    @StateId("count")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private Map<String, Row> inputRowsWithIds;

    public TokenizationFn(
        Schema schema,
        int batchSize,
        String rpcURI,
        TupleTag<FailsafeElement<Row, Row>> failureTag) {
      this.schema = schema;
      this.batchSize = batchSize;
      this.rpcURI = rpcURI;
      bufferedEvents = StateSpecs.bag(RowCoder.of(schema));
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

    @OnTimer("expiry")
    public void onExpiry(OnTimerContext context, @StateId("buffer") BagState<Row> bufferState) {
      boolean isEmpty = firstNonNull(bufferState.isEmpty().read(), true);
      if (!isEmpty) {
        processBufferedRows(bufferState.read(), context);
        bufferState.clear();
      }
    }

    @ProcessElement
    public void process(
        ProcessContext context,
        BoundedWindow window,
        @StateId("buffer") BagState<Row> bufferState,
        @StateId("count") ValueState<Integer> countState,
        @TimerId("expiry") Timer expiryTimer) {

      expiryTimer.set(window.maxTimestamp());

      int count = firstNonNull(countState.read(), 0);
      count++;
      countState.write(count);
      bufferState.add(context.element().getValue());

      if (count >= batchSize) {
        processBufferedRows(bufferState.read(), context);
        bufferState.clear();
        countState.clear();
      }
    }

    @SuppressWarnings("argument.type.incompatible")
    private void processBufferedRows(Iterable<Row> rows, WindowedContext context) {

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

    @SuppressWarnings("argument.type.incompatible")
    private ArrayList<Row> getTokenizedRow(Iterable<Row> inputRows) throws IOException {
      ArrayList<Row> outputRows = new ArrayList<>();

      CloseableHttpResponse response =
          sendRpc(formatJsonsToRpcBatch(rowsToJsons(inputRows)).getBytes(Charset.defaultCharset()));

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
