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
package org.apache.beam.examples.webapis;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.io.requestresponse.ApiIOError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;

/** A place for additional snippets to be included in the Beam website. */
class AdditionalSnippets {

  /** An example showing how to write {@link ApiIOError}s to BigQuery using {@link BigQueryIO}. */
  // [START webapis_java_write_failures_bigquery]
  static void writeFailuresToBigQuery(
      PCollection<ApiIOError> failures,
      TableReference tableReference,
      BigQueryIO.Write.CreateDisposition createDisposition,
      BigQueryIO.Write.WriteDisposition writeDisposition) {

    // PCollection<ApiIOError> failures = ...
    // TableReference tableReference = ...
    // BigQueryIO.Write.CreateDisposition createDisposition = ...
    // BigQueryIO.Write.WriteDisposition writeDisposition = ...

    failures.apply(
        "Dead letter",
        BigQueryIO.<ApiIOError>write()
            .useBeamSchema()
            .to(tableReference)
            .withCreateDisposition(createDisposition)
            .withWriteDisposition(writeDisposition));
  }
  // [END webapis_java_write_failures_bigquery]
}
