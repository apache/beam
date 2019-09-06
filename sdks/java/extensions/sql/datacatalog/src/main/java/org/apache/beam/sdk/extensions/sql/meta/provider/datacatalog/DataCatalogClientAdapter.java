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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import com.google.cloud.datacatalog.DataCatalogGrpc;
import com.google.cloud.datacatalog.DataCatalogGrpc.DataCatalogBlockingStub;
import com.google.cloud.datacatalog.Entry;
import com.google.cloud.datacatalog.LookupEntryRequest;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.auth.MoreCallCredentials;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/** Wraps DataCatalog GRPC client and exposes simplified APIS for Data Catalog Table Provider. */
class DataCatalogClientAdapter {

  private DataCatalogPipelineOptions options;

  private Supplier<DataCatalogBlockingStub> dcClient = () -> newClient(options);

  private DataCatalogClientAdapter(DataCatalogPipelineOptions options) {
    this.options = options;
  }

  /** Prod endpoint (default set in pipeline options): datacatalog.googleapis.com. */
  public static DataCatalogClientAdapter withOptions(DataCatalogPipelineOptions options) {
    return new DataCatalogClientAdapter(options);
  }

  private DataCatalogBlockingStub newClient(DataCatalogPipelineOptions options) {
    Channel authedChannel =
        ClientInterceptors.intercept(
            ManagedChannelBuilder.forTarget(options.getDataCatalogEndpoint()).build(),
            new CredentialsInterceptor());
    return DataCatalogGrpc.newBlockingStub(authedChannel);
  }

  public @Nullable Table getTable(String tableName) {
    Entry entry = dcClient.get().lookupEntry(sqlResource(tableName));
    return TableUtils.toBeamTable(tableName, entry);
  }

  private LookupEntryRequest sqlResource(String tableName) {
    return LookupEntryRequest.newBuilder().setSqlResource(tableName).build();
  }

  /** Provides credentials set up in PipelineOptions. */
  private final class CredentialsInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      CallCredentials callCredentials =
          MoreCallCredentials.from(options.as(GcpOptions.class).getGcpCredential());
      return next.newCall(method, callOptions.withCallCredentials(callCredentials));
    }
  }
}
