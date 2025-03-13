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
import com.google.protobuf.DescriptorProtos;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.DoFn;

/** Base dynamicDestinations class used by the Storage API sink. */
abstract class StorageApiDynamicDestinations<T, DestinationT>
    extends DynamicDestinationsHelpers.DelegatingDynamicDestinations<T, DestinationT> {
  public interface MessageConverter<T> {
    com.google.cloud.bigquery.storage.v1.TableSchema getTableSchema();

    DescriptorProtos.DescriptorProto getDescriptor(boolean includeCdcColumns) throws Exception;

    StorageApiWritePayload toMessage(
        T element, @Nullable RowMutationInformation rowMutationInformation) throws Exception;

    TableRow toFailsafeTableRow(T element);
  }

  StorageApiDynamicDestinations(DynamicDestinations<T, DestinationT> inner) {
    super(inner);
  }

  public abstract MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception;

  @Override
  void setSideInputAccessorFromProcessContext(DoFn<?, ?>.ProcessContext context) {
    super.setSideInputAccessorFromProcessContext(context);
    inner.setSideInputAccessorFromProcessContext(context);
  }
}
