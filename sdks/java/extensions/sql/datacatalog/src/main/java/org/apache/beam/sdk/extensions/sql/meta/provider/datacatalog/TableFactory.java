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
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/**
 * A {@link TableFactory} <i>may</i> be able to interpret a given Data Catalog {@link Entry} into
 * Beam SQL {@link Table}.
 */
interface TableFactory {

  /**
   * If this {@link TableFactory} instance can interpret the given {@link Entry}, then a Beam SQL
   * {@link Table} is constructed, else returns {@link Optional#empty}.
   *
   * <p>The {@link Table} is returned as a builder for further customization by the caller.
   */
  Optional<Table.Builder> tableBuilder(Entry entry);
}
