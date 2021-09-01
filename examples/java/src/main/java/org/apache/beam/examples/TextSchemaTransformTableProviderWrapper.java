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
package org.apache.beam.examples;

import com.google.auto.service.AutoService;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.meta.provider.SchemaTransformTableProviderWrapper;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection.IsBounded;

@AutoService(TableProvider.class)
public class TextSchemaTransformTableProviderWrapper extends SchemaTransformTableProviderWrapper {

  @Override
  public SchemaTransformProvider getReadTransformProvider() {
    return new TextReadTransformProvider();
  }

  @Override
  public SchemaTransformProvider getWriteTransformProvider() {
    return new TextWriteTransformProvider();
  }

  @Override
  public String getTableType() {
    return "text2"; // Resolve conflict with actual text one.
  }

  @Override
  public String getLocationConfigName() {
    return "location";
  }

  @Nullable
  @Override
  public String getSchemaConfigName() {
    return "schema";
  }

  @Override
  public boolean schemaConfigOnlyOnRead() {
    return true;
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }
}
