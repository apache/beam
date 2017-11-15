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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * PTransform that performs streaming BigQuery write. To increase consistency, it leverages
 * BigQuery's best effort de-dup mechanism.
 */
public class StreamingInserts<T, DestinationT>
    extends PTransform<PCollection<KV<DestinationT, T>>, WriteResult> {
  private final BigQueryServices bigQueryServices;
  private final CreateDisposition createDisposition;
  private final DynamicDestinations<T, DestinationT> dynamicDestinations;
  private final Coder<T> inputCoder;
  private final SerializableFunction<T, TableRow> formatFunction;
  private InsertRetryPolicy retryPolicy;

  /** Constructor. */
  public StreamingInserts(
      CreateDisposition createDisposition,
      DynamicDestinations<T, DestinationT> dynamicDestinations,
      Coder<T> inputCoder,
      SerializableFunction<T, TableRow> formatFunction) {
    this(
        createDisposition,
        dynamicDestinations,
        inputCoder,
        new BigQueryServicesImpl(),
        formatFunction,
        InsertRetryPolicy.alwaysRetry());
  }

  /** Constructor. */
  private StreamingInserts(
      CreateDisposition createDisposition,
      DynamicDestinations<T, DestinationT> dynamicDestinations,
      Coder<T> inputCoder,
      BigQueryServices bigQueryServices,
      SerializableFunction<T, TableRow> formatFunction,
      InsertRetryPolicy retryPolicy) {
    this.createDisposition = createDisposition;
    this.dynamicDestinations = dynamicDestinations;
    this.inputCoder = inputCoder;
    this.bigQueryServices = bigQueryServices;
    this.formatFunction = formatFunction;
    this.retryPolicy = retryPolicy;
  }

  /**
   * Specify a retry policy for failed inserts.
   */
  public StreamingInserts<T, DestinationT> withInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
    return new StreamingInserts<>(
        createDisposition,
        dynamicDestinations,
        inputCoder,
        bigQueryServices,
        formatFunction,
        retryPolicy);
  }

  StreamingInserts<T, DestinationT> withTestServices(BigQueryServices bigQueryServices) {
    return new StreamingInserts<>(
        createDisposition,
        dynamicDestinations,
        inputCoder,
        bigQueryServices,
        formatFunction,
        retryPolicy);
  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, T>> input) {
    PCollection<KV<String, T>> writes =
        input.apply(
            "CreateTables",
            new CreateTables<>(createDisposition, dynamicDestinations)
                .withTestServices(bigQueryServices));

    return writes.apply(
        new StreamingWriteTables<>(formatFunction, inputCoder)
            .withTestServices(bigQueryServices)
            .withInsertRetryPolicy(retryPolicy));
  }
}
