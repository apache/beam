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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.provider.SchemaCapableIOTableProviderWrapper;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaCapableIOProvider;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;

/**
 * {@link TableProvider} for {@link PubsubIO} for consumption by Beam SQL.
 *
 * <p>Passes the {@link PubsubSchemaCapableIOProvider} to the generalized table provider wrapper,
 * {@link SchemaCapableIOTableProviderWrapper}, for Pubsub specific behavior.
 */
@Internal
@Experimental
@AutoService(TableProvider.class)
public class PubsubJsonTableProvider extends SchemaCapableIOTableProviderWrapper {
  @Override
  public SchemaIOProvider getSchemaIOProvider() {
    return new PubsubSchemaCapableIOProvider();
  }

  @Override
  public String getTableType() {
    return "pubsub";
  }
}
