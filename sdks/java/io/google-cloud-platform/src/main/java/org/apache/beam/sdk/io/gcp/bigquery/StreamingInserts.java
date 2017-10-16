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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * PTransform that performs streaming BigQuery write. To increase consistency, it leverages
 * BigQuery's best effort de-dup mechanism.
 */
public class StreamingInserts<DestinationT>
    extends PTransform<PCollection<KV<DestinationT, TableRow>>, WriteResult> {
  private BigQueryServices bigQueryServices;
  private final CreateDisposition createDisposition;
  private final DynamicDestinations<?, DestinationT> dynamicDestinations;
  private InsertRetryPolicy retryPolicy;

  /** Constructor. */
  public StreamingInserts(CreateDisposition createDisposition,
                   DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this(createDisposition, dynamicDestinations, new BigQueryServicesImpl(),
        InsertRetryPolicy.alwaysRetry());
  }

  /** Constructor. */
  private StreamingInserts(CreateDisposition createDisposition,
                          DynamicDestinations<?, DestinationT> dynamicDestinations,
                          BigQueryServices bigQueryServices,
                          InsertRetryPolicy retryPolicy) {
    this.createDisposition = createDisposition;
    this.dynamicDestinations = dynamicDestinations;
    this.bigQueryServices = bigQueryServices;
    this.retryPolicy = retryPolicy;
  }

  /**
   * Specify a retry policy for failed inserts.
   */
  public StreamingInserts<DestinationT> withInsertRetryPolicy(InsertRetryPolicy retryPolicy) {
    return new StreamingInserts<>(
        createDisposition, dynamicDestinations, bigQueryServices, retryPolicy);
  }

  StreamingInserts<DestinationT> withTestServices(BigQueryServices bigQueryServices) {
    return new StreamingInserts<>(
        createDisposition, dynamicDestinations, bigQueryServices, retryPolicy);  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, TableRow>> input) {
    PCollection<KV<TableDestination, TableRow>> writes =
        input.apply(
            "CreateTables",
            new CreateTables<>(createDisposition, dynamicDestinations)
                .withTestServices(bigQueryServices));

    return writes.apply(
        new StreamingWriteTables()
            .withTestServices(bigQueryServices)
            .withInsertRetryPolicy(retryPolicy));
  }
}
