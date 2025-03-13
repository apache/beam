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

import com.google.cloud.datacatalog.v1beta1.Entry;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/** {@link TableFactory} that uses the first applicable sub-{@link TableFactory}. */
class ChainedTableFactory implements TableFactory {

  private final List<TableFactory> subTableFactories;

  public static ChainedTableFactory of(TableFactory... subTableFactories) {
    return new ChainedTableFactory(Arrays.asList(subTableFactories));
  }

  private ChainedTableFactory(List<TableFactory> subTableFactories) {
    this.subTableFactories = subTableFactories;
  }

  /** Creates a Beam SQL table description from a GCS fileset entry. */
  @Override
  public Optional<Table.Builder> tableBuilder(Entry entry) {
    for (TableFactory tableFactory : subTableFactories) {
      Optional<Table.Builder> tableBuilder = tableFactory.tableBuilder(entry);
      if (tableBuilder.isPresent()) {
        return tableBuilder;
      }
    }
    return Optional.empty();
  }
}
