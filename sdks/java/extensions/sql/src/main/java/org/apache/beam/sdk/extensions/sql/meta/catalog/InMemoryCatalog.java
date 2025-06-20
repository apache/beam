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
package org.apache.beam.sdk.extensions.sql.meta.catalog;

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.util.Preconditions;

public class InMemoryCatalog implements Catalog {
  private final String name;
  private final Map<String, String> properties;
  private final InMemoryMetaStore metaStore = new InMemoryMetaStore();

  public InMemoryCatalog(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
  }

  @Override
  public String type() {
    return "local";
  }

  @Override
  public String name() {
    return Preconditions.checkStateNotNull(
        name, getClass().getSimpleName() + " has not been initialized");
  }

  @Override
  public MetaStore metaStore() {
    return metaStore;
  }

  @Override
  public Map<String, String> properties() {
    return Preconditions.checkStateNotNull(properties, "InMemoryCatalog has not been initialized");
  }
}
