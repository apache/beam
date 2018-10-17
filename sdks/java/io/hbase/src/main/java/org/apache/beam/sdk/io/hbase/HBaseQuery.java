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
package org.apache.beam.sdk.io.hbase;

import org.apache.hadoop.hbase.client.Scan;

/**
 * A class to encapsulate a query to HBase. It is composed by the id of the table and the {@link
 * Scan} object.
 */
public final class HBaseQuery {

  public static HBaseQuery of(String tableId, Scan scan) {
    return new HBaseQuery(tableId, scan);
  }

  private HBaseQuery(String tableId, Scan scan) {
    this.tableId = tableId;
    this.scan = scan;
  }

  public String getTableId() {
    return tableId;
  }

  public Scan getScan() {
    return scan;
  }

  @Override
  public String toString() {
    return "HBaseQuery{" + "tableId='" + tableId + '\'' + ", scan=" + scan + '}';
  }

  private final String tableId;
  private final Scan scan;
}
