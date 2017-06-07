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
import org.apache.beam.sdk.coders.VoidCoder;
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
  private final String partition;

  /** Constructor. */
  StreamingInserts(CreateDisposition createDisposition,
                   DynamicDestinations<?, DestinationT> dynamicDestinations,
                   String partition) {
    this.createDisposition = createDisposition;
    this.dynamicDestinations = dynamicDestinations;
    this.partition = partition;
    this.bigQueryServices = new BigQueryServicesImpl();
  }

  void setTestServices(BigQueryServices bigQueryServices) {
    this.bigQueryServices = bigQueryServices;
  }

  @Override
  protected Coder<Void> getDefaultOutputCoder() {
    return VoidCoder.of();
  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, TableRow>> input) {
    PCollection<KV<TableDestination, TableRow>> writes =
        input.apply(
            "CreateTables",
            new CreateTables<DestinationT>(createDisposition, dynamicDestinations, partition)
              .withTestServices(bigQueryServices));

    return writes.apply(new StreamingWriteTables().withTestServices(bigQueryServices));
  }
}
