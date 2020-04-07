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
package org.apache.beam.sdk.io.gcp.healthcare;

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIOTestUtil.PRETTY_BUNDLES;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOWriteIT {

  private FhirIOTestOptions options;
  private transient HealthcareApiClient client;

  @Before
  public void setup() throws Exception {
    if (client == null) {
      client = new HttpHealthcareApiClient();
    }
    PipelineOptionsFactory.register(FhirIOTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(FhirIOTestOptions.class);
    options.setGcsTempPath("gs://jferriero-dev/FhirIOWriteIT/temp/");
    options.setGcsDeadLetterPath("gs://jferriero-dev/FhirIOWriteIT/deadletter/");
    options.setFhirStore(
        "projects/jferriero-dev/locations/us-central1/datasets/raw-dataset/fhirStores/raw-fhir-store");
  }

  @Test
  public void testFhirIO_ExecuteBundle() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    FhirIO.Write.Result writeResult =
        pipeline
            .apply(Create.of(PRETTY_BUNDLES).withCoder(new HttpBodyCoder()))
            .apply(FhirIO.Write.executeBundles(options.getFhirStore()));

    PAssert.that(writeResult.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFhirIO_Import() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    FhirIO.Write.Result result =
        pipeline
            .apply(Create.of(PRETTY_BUNDLES).withCoder(new HttpBodyCoder()))
            .apply(
                FhirIO.Write.fhirStoresImport(
                    options.getFhirStore(),
                    options.getGcsTempPath(),
                    options.getGcsDeadLetterPath(),
                    ContentStructure.BUNDLE));

    PAssert.that(result.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
  }
}
