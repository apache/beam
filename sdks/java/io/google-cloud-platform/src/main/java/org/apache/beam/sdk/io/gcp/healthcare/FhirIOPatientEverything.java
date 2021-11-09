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

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIO.BASE_METRIC_PREFIX;

import com.google.gson.JsonArray;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOPatientEverything.PatientEverythingParameter;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.FhirResourcePagesIterator;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirIOPatientEverything
    extends PTransform<PCollection<PatientEverythingParameter>, FhirIOPatientEverything.Result> {

  public static final TupleTag<JsonArray> OUT = new TupleTag<JsonArray>() {};
  public static final TupleTag<HealthcareIOError<String>> FAILED_READS =
      new TupleTag<HealthcareIOError<String>>() {};

  @DefaultCoder(PatientEverythingParameterCoder.class)
  public static class PatientEverythingParameter implements Serializable {

    private final String resourceName;
    @Nullable private final Map<String, String> filters;

    PatientEverythingParameter(String resourceName, @Nullable Map<String, String> filters) {
      this.resourceName = resourceName;
      this.filters = filters;
    }

    public static PatientEverythingParameter of(String resourceName) {
      return new PatientEverythingParameter(resourceName, null);
    }

    public static PatientEverythingParameter of(String resourceName, Map<String, String> filters) {
      return new PatientEverythingParameter(resourceName, filters);
    }

    public String getResourceName() {
      return resourceName;
    }

    public Map<String, String> getFilters() {
      return filters;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PatientEverythingParameter that = (PatientEverythingParameter) o;
      return Objects.equals(resourceName, that.getResourceName())
          && Objects.equals(filters, that.getFilters());
    }

    @Override
    public int hashCode() {
      return Objects.hash(resourceName, filters);
    }

    @Override
    public String toString() {
      return String.format(
          "FhirIOPatientEverything.Parameter{resourcePath='%s', filters='%s'}'",
          resourceName, filters);
    }
  }

  public static class PatientEverythingParameterCoder extends CustomCoder<PatientEverythingParameter> {

    private static final NullableCoder<String> STRING_CODER = NullableCoder
        .of(StringUtf8Coder.of());
    private static final NullableCoder<Map<String, String>> MAP_CODER = NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    public static PatientEverythingParameterCoder of() {
      return new PatientEverythingParameterCoder();
    }

    @Override
    public void encode(PatientEverythingParameter value, OutputStream outStream) throws IOException {
      STRING_CODER.encode(value.getResourceName(), outStream);
      MAP_CODER.encode(value.getFilters(), outStream);
    }

    @Override
    public PatientEverythingParameter decode(InputStream inStream) throws IOException {
      String resourceName = STRING_CODER.decode(inStream);
      Map<String, String> queries = MAP_CODER.decode(inStream);
      return PatientEverythingParameter.of(resourceName, queries);
    }
  }

  public static class Result implements POutput, PInput {

    private final PCollection<JsonArray> resources;
    private final PCollection<HealthcareIOError<String>> failedReads;

    PCollectionTuple pct;

    /**
     * Create FhirIO.Search.Result form PCollectionTuple with OUT and DEAD_LETTER tags.
     *
     * @param pct the pct
     * @return the search result
     * @throws IllegalArgumentException the illegal argument exception
     */
    static Result of(PCollectionTuple pct) throws IllegalArgumentException {
      if (pct.has(OUT) && pct.has(FAILED_READS)) {
        return new Result(pct);
      } else {
        throw new IllegalArgumentException(
            "The PCollection tuple must have the FhirIO.Search.OUT "
                + "and FhirIO.Search.DEAD_LETTER tuple tags");
      }
    }

    private Result(PCollectionTuple pct) {
      this.pct = pct;
      this.resources = pct.get(OUT).setCoder(JsonArrayCoder.of());
      this.failedReads =
          pct.get(FAILED_READS).setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
    }

    /**
     * Gets failed reads.
     *
     * @return the failed reads
     */
    public PCollection<HealthcareIOError<String>> getFailedReads() {
      return failedReads;
    }

    /**
     * Gets rad resources.
     *
     * @return the read resources
     */
    public PCollection<JsonArray> getResources() {
      return resources;
    }

    @Override
    public Pipeline getPipeline() {
      return this.pct.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(OUT, resources, FAILED_READS, failedReads);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  @Override
  public Result expand(PCollection<PatientEverythingParameter> input) {
    PCollectionTuple results =
        input.apply(
            "GetPatientEverything",
            ParDo.of(new GetPatientEverythingFn())
                .withOutputTags(OUT, TupleTagList.of(FAILED_READS)));
    return new Result(results);
  }

  static class GetPatientEverythingFn extends DoFn<PatientEverythingParameter, JsonArray> {

    private final Counter GET_PATIENT_EVERYTHING_ERROR_COUNT =
        Metrics.counter(
            GetPatientEverythingFn.class,
            BASE_METRIC_PREFIX + "get_patient_everything_error_count");
    private final Counter GET_PATIENT_EVERYTHING_SUCCESS_COUNT =
        Metrics.counter(
            GetPatientEverythingFn.class,
            BASE_METRIC_PREFIX + "get_patient_everything_success_count");
    private final Distribution GET_PATIENT_EVERYTHING_LATENCY_MS =
        Metrics.distribution(
            GetPatientEverythingFn.class, BASE_METRIC_PREFIX + "get_patient_everything_latency_ms");
    private final Logger LOG = LoggerFactory.getLogger(GetPatientEverythingFn.class);

    private HealthcareApiClient client;

    /**
     * Instantiate healthcare client.
     *
     * @throws IOException the io exception
     */
    @Setup
    public void instantiateHealthcareClient() throws IOException {
      this.client = new HttpHealthcareApiClient();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      PatientEverythingParameter patientEverythingParameter = context.element();
      try {
        context.output(getPatientEverything(patientEverythingParameter.getResourceName(), patientEverythingParameter
            .getFilters()));
      } catch (IllegalArgumentException | NoSuchElementException e) {
        GET_PATIENT_EVERYTHING_ERROR_COUNT.inc();
        LOG.warn(
            String.format(
                "Error search FHIR resources writing to Dead Letter "
                    + "Queue. Cause: %s Stack Trace: %s",
                e.getMessage(), Throwables.getStackTraceAsString(e)));
        context.output(FhirIO.Search.DEAD_LETTER, HealthcareIOError.of(patientEverythingParameter.toString(), e));
      }
    }

    private JsonArray getPatientEverything(
        String resourceName, @Nullable Map<String, String> filters) {
      long start = Instant.now().toEpochMilli();

      HashMap<String, Object> filterObjects = new HashMap<>();
      if (filters != null) {
        filters.forEach(filterObjects::put);
      }
      FhirResourcePagesIterator iter =
          FhirResourcePagesIterator.ofPatientEverything(client, resourceName, filterObjects);
      JsonArray result = new JsonArray();
      while (iter.hasNext()) {
        result.addAll(iter.next());
      }
      GET_PATIENT_EVERYTHING_LATENCY_MS.update(java.time.Instant.now().toEpochMilli() - start);
      GET_PATIENT_EVERYTHING_SUCCESS_COUNT.inc();
      return result;
    }
  }
}
