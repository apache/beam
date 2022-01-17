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

import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Base dynamicDestinations class used by the Storage API sink. */
abstract class StorageApiDynamicDestinations<T, DestinationT>
    extends DynamicDestinations<T, DestinationT> {
  public interface MessageConverter<T> {
    Descriptor getSchemaDescriptor();

    Message toMessage(T element);
  }

  private DynamicDestinations<T, DestinationT> inner;

  StorageApiDynamicDestinations(DynamicDestinations<T, DestinationT> inner) {
    this.inner = inner;
  }

  public abstract MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception;

  @Override
  public DestinationT getDestination(ValueInSingleWindow<T> element) {
    return inner.getDestination(element);
  }

  @Override
  public @Nullable Coder<DestinationT> getDestinationCoder() {
    return inner.getDestinationCoder();
  }

  @Override
  public TableDestination getTable(DestinationT destination) {
    return inner.getTable(destination);
  }

  @Override
  public TableSchema getSchema(DestinationT destination) {
    return inner.getSchema(destination);
  }

  @Override
  public List<PCollectionView<?>> getSideInputs() {
    return inner.getSideInputs();
  }

  @Override
  void setSideInputAccessorFromProcessContext(DoFn<?, ?>.ProcessContext context) {
    super.setSideInputAccessorFromProcessContext(context);
    inner.setSideInputAccessorFromProcessContext(context);
  }
}
