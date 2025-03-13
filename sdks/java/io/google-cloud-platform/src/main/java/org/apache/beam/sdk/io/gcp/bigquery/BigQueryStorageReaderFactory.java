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

import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.io.IOException;

class BigQueryStorageReaderFactory {

  private BigQueryStorageReaderFactory() {}

  public static BigQueryStorageReader getReader(ReadSession readSession) throws IOException {
    if (readSession.hasAvroSchema()) {
      return new BigQueryStorageAvroReader(readSession);
    } else if (readSession.hasArrowSchema()) {
      return new BigQueryStorageArrowReader(readSession);
    }
    throw new IllegalStateException("Read session does not have Avro/Arrow schema set.");
  }
}
