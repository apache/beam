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

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A transform that converts messages to protocol buffers in preparation for writing to BigQuery.
 */
public class StorageApiConvertMessages<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, PCollectionTuple> {
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;
  private final TupleTag<BigQueryStorageApiInsertError> failedWritesTag;
  private final TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag;
  private final Coder<BigQueryStorageApiInsertError> errorCoder;
  private final Coder<KV<DestinationT, StorageApiWritePayload>> successCoder;

  private final @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn;
  private final BadRecordRouter badRecordRouter;

  public StorageApiConvertMessages(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      TupleTag<BigQueryStorageApiInsertError> failedWritesTag,
      TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag,
      Coder<BigQueryStorageApiInsertError> errorCoder,
      Coder<KV<DestinationT, StorageApiWritePayload>> successCoder,
      @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn,
      BadRecordRouter badRecordRouter) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
    this.failedWritesTag = failedWritesTag;
    this.successfulWritesTag = successfulWritesTag;
    this.errorCoder = errorCoder;
    this.successCoder = successCoder;
    this.rowMutationFn = rowMutationFn;
    this.badRecordRouter = badRecordRouter;
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<DestinationT, ElementT>> input) {
    String operationName = input.getName() + "/" + getName();

    PCollectionTuple result =
        input.apply(
            "Convert to message",
            ParDo.of(
                    new ConvertMessagesDoFn<>(
                        dynamicDestinations,
                        bqServices,
                        operationName,
                        failedWritesTag,
                        successfulWritesTag,
                        rowMutationFn,
                        badRecordRouter,
                        input.getCoder()))
                .withOutputTags(
                    successfulWritesTag,
                    TupleTagList.of(ImmutableList.of(failedWritesTag, BAD_RECORD_TAG)))
                .withSideInputs(dynamicDestinations.getSideInputs()));
    result.get(successfulWritesTag).setCoder(successCoder);
    result.get(failedWritesTag).setCoder(errorCoder);
    result.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline()));
    return result;
  }

  public static class ConvertMessagesDoFn<DestinationT extends @NonNull Object, ElementT>
      extends DoFn<KV<DestinationT, ElementT>, KV<DestinationT, StorageApiWritePayload>> {
    private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
    private TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
    private final BigQueryServices bqServices;
    private final TupleTag<BigQueryStorageApiInsertError> failedWritesTag;
    private final TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag;
    private final @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn;
    private final BadRecordRouter badRecordRouter;
    Coder<KV<DestinationT, ElementT>> elementCoder;
    private transient @Nullable DatasetService datasetServiceInternal = null;

    ConvertMessagesDoFn(
        StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
        BigQueryServices bqServices,
        String operationName,
        TupleTag<BigQueryStorageApiInsertError> failedWritesTag,
        TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag,
        @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn,
        BadRecordRouter badRecordRouter,
        Coder<KV<DestinationT, ElementT>> elementCoder) {
      this.dynamicDestinations = dynamicDestinations;
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.bqServices = bqServices;
      this.failedWritesTag = failedWritesTag;
      this.successfulWritesTag = successfulWritesTag;
      this.rowMutationFn = rowMutationFn;
      this.badRecordRouter = badRecordRouter;
      this.elementCoder = elementCoder;
    }

    private DatasetService getDatasetService(PipelineOptions pipelineOptions) throws IOException {
      if (datasetServiceInternal == null) {
        datasetServiceInternal =
            bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
      }
      return datasetServiceInternal;
    }

    @Teardown
    public void onTeardown() {
      try {
        if (datasetServiceInternal != null) {
          datasetServiceInternal.close();
          datasetServiceInternal = null;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        PipelineOptions pipelineOptions,
        @Element KV<DestinationT, ElementT> element,
        @Timestamp Instant timestamp,
        MultiOutputReceiver o)
        throws Exception {
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      MessageConverter<ElementT> messageConverter =
          messageConverters.get(
              element.getKey(), dynamicDestinations, getDatasetService(pipelineOptions));

      RowMutationInformation rowMutationInformation = null;
      if (rowMutationFn != null) {
        rowMutationInformation =
            Preconditions.checkStateNotNull(rowMutationFn).apply(element.getValue());
      }
      try {
        StorageApiWritePayload payload =
            messageConverter
                .toMessage(element.getValue(), rowMutationInformation)
                .withTimestamp(timestamp);
        o.get(successfulWritesTag).output(KV.of(element.getKey(), payload));
      } catch (TableRowToStorageApiProto.SchemaConversionException conversionException) {
        TableRow failsafeTableRow;
        try {
          failsafeTableRow = messageConverter.toFailsafeTableRow(element.getValue());
        } catch (Exception e) {
          badRecordRouter.route(o, element, elementCoder, e, "Unable to convert value to TableRow");
          return;
        }
        o.get(failedWritesTag)
            .output(
                new BigQueryStorageApiInsertError(
                    failsafeTableRow, conversionException.toString()));
      } catch (Exception e) {
        badRecordRouter.route(
            o, element, elementCoder, e, "Unable to convert value to StorageWriteApiPayload");
      }
    }
  }
}
