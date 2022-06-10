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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOPatientEverything.PatientEverythingParameter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void test_FhirIO_failedReads() {
    List<String> badMessageIDs = Arrays.asList("foo", "bar");
    FhirIO.Read.Result readResult =
        pipeline.apply(Create.of(badMessageIDs)).apply(FhirIO.readResources());

    PCollection<HealthcareIOError<String>> failed = readResult.getFailedReads();

    PCollection<String> resources = readResult.getResources();

    PCollection<String> failedMsgIds =
        failed.apply(
            MapElements.into(TypeDescriptors.strings()).via(HealthcareIOError::getDataResource));

    PAssert.that(failedMsgIds).containsInAnyOrder(badMessageIDs);
    PAssert.that(resources).empty();
    pipeline.run();
  }

  @Test
  public void test_FhirIO_failedSearches() {
    FhirSearchParameter<String> input = FhirSearchParameter.of("resource-type-1", null);
    FhirIO.Search.Result searchResult =
        pipeline
            .apply(Create.of(input).withCoder(FhirSearchParameterCoder.of(StringUtf8Coder.of())))
            .apply(FhirIO.searchResources("bad-store"));

    PCollection<HealthcareIOError<String>> failed = searchResult.getFailedSearches();

    PCollection<String> failedMsgIds =
        failed.apply(
            MapElements.into(TypeDescriptors.strings()).via(HealthcareIOError::getDataResource));

    PAssert.that(failedMsgIds).containsInAnyOrder(input.toString());
    PAssert.that(searchResult.getResources()).empty();
    PAssert.that(searchResult.getKeyedResources()).empty();
    pipeline.run();
  }

  @Test
  public void test_FhirIO_failedSearchesWithGenericParameters() {
    FhirSearchParameter<List<String>> input = FhirSearchParameter.of("resource-type-1", null);
    FhirIO.Search.Result searchResult =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FhirSearchParameterCoder.of(ListCoder.of(StringUtf8Coder.of()))))
            .apply(
                (FhirIO.Search<List<String>>)
                    FhirIO.searchResourcesWithGenericParameters("bad-store"));

    PCollection<HealthcareIOError<String>> failed = searchResult.getFailedSearches();

    PCollection<String> failedMsgIds =
        failed.apply(
            MapElements.into(TypeDescriptors.strings()).via(HealthcareIOError::getDataResource));

    PAssert.that(failedMsgIds).containsInAnyOrder(input.toString());
    PAssert.that(searchResult.getResources()).empty();
    PAssert.that(searchResult.getKeyedResources()).empty();
    pipeline.run();
  }

  @Test
  public void test_FhirIO_failedWrites() {
    String badBundle = "bad";
    List<String> emptyMessages = Collections.singletonList(badBundle);

    PCollection<String> fhirBundles = pipeline.apply(Create.of(emptyMessages));

    FhirIO.Write.AbstractResult writeResult =
        fhirBundles.apply(
            FhirIO.Write.executeBundles(
                "projects/foo/locations/us-central1/datasets/bar/hl7V2Stores/baz"));

    PCollection<HealthcareIOError<String>> failedInserts = writeResult.getFailedBodies();

    PAssert.thatSingleton(failedInserts)
        .satisfies(
            (HealthcareIOError<String> err) -> {
              Assert.assertEquals("bad", err.getDataResource());
              return null;
            });
    PCollection<Long> numFailedInserts = failedInserts.apply(Count.globally());

    PAssert.thatSingleton(numFailedInserts).isEqualTo(1L);

    pipeline.run();
  }

  @Test
  public void test_FhirIO_failedPatientEverything() {
    PatientEverythingParameter input =
        PatientEverythingParameter.builder().setResourceName("bad-resource-name").build();
    FhirIOPatientEverything.Result everythingResult =
        pipeline.apply(Create.of(input)).apply(FhirIO.getPatientEverything());

    PCollection<HealthcareIOError<String>> failed = everythingResult.getFailedReads();
    PCollection<String> failedEverything =
        failed.apply(
            MapElements.into(TypeDescriptors.strings()).via(HealthcareIOError::getDataResource));

    PAssert.that(failedEverything).containsInAnyOrder(input.toString());
    PAssert.that(everythingResult.getPatientCompartments()).empty();
    pipeline.run();
  }

  @Test
  public void test_FhirIO_Export_invalidUri() {
    final String invalidUri = "someInvalidUri";
    pipeline.apply(FhirIO.exportResources("fakeStore", invalidUri));
    final RuntimeException exceptionThrown = assertThrows(RuntimeException.class, pipeline::run);
    assertTrue(exceptionThrown.getMessage().contains(invalidUri));
  }
}
