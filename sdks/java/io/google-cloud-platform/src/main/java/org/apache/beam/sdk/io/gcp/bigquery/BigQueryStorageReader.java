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

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;

interface BigQueryStorageReader extends AutoCloseable {

  void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException;

  long getRowCount();

  // TODO(https://github.com/apache/beam/issues/21076): BigQueryStorageReader should produce Rows,
  // rather than GenericRecords
  GenericRecord readSingleRecord() throws IOException;

  boolean readyForNextReadResponse() throws IOException;

  void resetBuffer();

  @Override
  void close();
}
