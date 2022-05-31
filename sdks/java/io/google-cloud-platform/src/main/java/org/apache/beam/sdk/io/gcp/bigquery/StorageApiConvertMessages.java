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

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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

  public StorageApiConvertMessages(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      TupleTag<BigQueryStorageApiInsertError> failedWritesTag,
      TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag,
      Coder<BigQueryStorageApiInsertError> errorCoder,
      Coder<KV<DestinationT, StorageApiWritePayload>> successCoder) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
    this.failedWritesTag = failedWritesTag;
    this.successfulWritesTag = successfulWritesTag;
    this.errorCoder = errorCoder;
    this.successCoder = successCoder;
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
                        successfulWritesTag))
                .withOutputTags(successfulWritesTag, TupleTagList.of(failedWritesTag))
                .withSideInputs(dynamicDestinations.getSideInputs()));
    result.get(successfulWritesTag).setCoder(successCoder);
    result.get(failedWritesTag).setCoder(errorCoder);
    return result;
  }

  public static class ConvertMessagesDoFn<DestinationT extends @NonNull Object, ElementT>
      extends DoFn<KV<DestinationT, ElementT>, KV<DestinationT, StorageApiWritePayload>> {
    private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
    private TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
    private final BigQueryServices bqServices;
    private final TupleTag<BigQueryStorageApiInsertError> failedWritesTag;
    private final TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag;
    private transient @Nullable DatasetService datasetServiceInternal = null;

    ConvertMessagesDoFn(
        StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
        BigQueryServices bqServices,
        String operationName,
        TupleTag<BigQueryStorageApiInsertError> failedWritesTag,
        TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag) {
      this.dynamicDestinations = dynamicDestinations;
      this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
      this.bqServices = bqServices;
      this.failedWritesTag = failedWritesTag;
      this.successfulWritesTag = successfulWritesTag;
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
        MultiOutputReceiver o)
        throws Exception {
      dynamicDestinations.setSideInputAccessorFromProcessContext(c);
      MessageConverter<ElementT> messageConverter =
          messageConverters.get(
              element.getKey(), dynamicDestinations, getDatasetService(pipelineOptions));
      try {
        StorageApiWritePayload payload = messageConverter.toMessage(element.getValue());
        o.get(successfulWritesTag).output(KV.of(element.getKey(), payload));
      } catch (TableRowToStorageApiProto.SchemaConversionException e) {
        TableRow tableRow = messageConverter.toTableRow(element.getValue());
        o.get(failedWritesTag).output(new BigQueryStorageApiInsertError(tableRow, e.toString()));
      }
    }
  }
}
