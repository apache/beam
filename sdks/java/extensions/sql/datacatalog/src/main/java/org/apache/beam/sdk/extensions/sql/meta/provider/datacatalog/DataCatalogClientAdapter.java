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

import com.google.auth.oauth2.GoogleCredentials;
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
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/** Wraps DataCatalog GRPC client and exposes simplified APIS for Data Catalog Table Provider. */
class DataCatalogClientAdapter {

  private DataCatalogBlockingStub dcClient;

  private DataCatalogClientAdapter(DataCatalogBlockingStub dcClient) {
    this.dcClient = dcClient;
  }

  /** Prod endpoint (default set in pipeline options): datacatalog.googleapis.com. */
  public static DataCatalogClientAdapter withDefaultCredentials(String endpoint)
      throws IOException {
    return new DataCatalogClientAdapter(newClient(endpoint));
  }

  private static DataCatalogBlockingStub newClient(String endpoint) throws IOException {
    Channel authedChannel =
        ClientInterceptors.intercept(
            ManagedChannelBuilder.forTarget(endpoint).build(),
            CredentialsInterceptor.defaultCredentials());
    return DataCatalogGrpc.newBlockingStub(authedChannel);
  }

  public @Nullable Table getTable(String tableName) {
    Entry entry = dcClient.lookupEntry(sqlResource(tableName));
    return TableUtils.toBeamTable(tableName, entry);
  }

  private LookupEntryRequest sqlResource(String tableName) {
    return LookupEntryRequest.newBuilder().setSqlResource(tableName).build();
  }

  /** Provides default credentials. */
  private static final class CredentialsInterceptor implements ClientInterceptor {

    private CallCredentials callCredentials;

    private CredentialsInterceptor(CallCredentials callCredentials) {
      this.callCredentials = callCredentials;
    }

    public static CredentialsInterceptor defaultCredentials() throws IOException {
      GoogleCredentials defaultCredentials = GoogleCredentials.getApplicationDefault();
      return of(MoreCallCredentials.from(defaultCredentials));
    }

    public static CredentialsInterceptor of(CallCredentials credentials) {
      return new CredentialsInterceptor(credentials);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return next.newCall(method, callOptions.withCallCredentials(callCredentials));
    }
  }
}
