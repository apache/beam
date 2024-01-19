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

import com.google.auto.value.AutoValue;
import com.google.gson.JsonArray;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOPatientEverything.PatientEverythingParameter;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.FhirResourcePagesIterator;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The type FhirIOPatientEverything for querying a FHIR Patient resource's compartment. * */
public class FhirIOPatientEverything
    extends PTransform<PCollection<PatientEverythingParameter>, FhirIOPatientEverything.Result> {

  /** The tag for the main output of FHIR Resources from a GetPatientEverything request. */
  public static final TupleTag<JsonArray> OUT = new TupleTag<JsonArray>() {};
  /** The tag for the deadletter output of FHIR Resources from a GetPatientEverything request. */
  public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
      new TupleTag<HealthcareIOError<String>>() {};

  /**
   * PatientEverythingParameter defines required attributes for a FHIR GetPatientEverything request
   * in {@link FhirIOPatientEverything}. *
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class PatientEverythingParameter implements Serializable {

    /**
     * FHIR Patient resource name in the format of
     * projects/{p}/locations/{l}/datasets/{d}/fhirStores/{f}/fhir/{resourceType}/{id}.
     */
    abstract String getResourceName();
    /** Optional filters for the request, eg. start, end, _type, _since, _count */
    abstract @Nullable Map<String, String> getFilters();

    static Builder builder() {
      return new AutoValue_FhirIOPatientEverything_PatientEverythingParameter.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setResourceName(String resourceName);

      abstract Builder setFilters(Map<String, String> query);

      abstract PatientEverythingParameter build();
    }
  }

  /** The Result for a {@link FhirIOPatientEverything} request. */
  public static class Result implements POutput, PInput {

    private final PCollection<JsonArray> patientCompartments;
    private final PCollection<HealthcareIOError<String>> failedReads;

    PCollectionTuple pct;

    /**
     * Create FhirIOPatientEverything.Result form PCollectionTuple with OUT and DEAD_LETTER tags.
     *
     * @param pct the pct
     * @return the patient everything result
     * @throws IllegalArgumentException the illegal argument exception
     */
    static Result of(PCollectionTuple pct) throws IllegalArgumentException {
      if (pct.has(OUT) && pct.has(DEAD_LETTER)) {
        return new Result(pct);
      } else {
        throw new IllegalArgumentException(
            "The PCollection tuple must have the FhirIOPatientEverything.OUT "
                + "and FhirIOPatientEverything.DEAD_LETTER tuple tags");
      }
    }

    private Result(PCollectionTuple pct) {
      this.pct = pct;
      this.patientCompartments = pct.get(OUT).setCoder(JsonArrayCoder.of());
      this.failedReads =
          pct.get(DEAD_LETTER).setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
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
     * Gets the patient compartment responses for GetPatientEverything requests.
     *
     * @return the read patient compartments
     */
    public PCollection<JsonArray> getPatientCompartments() {
      return patientCompartments;
    }

    @Override
    public Pipeline getPipeline() {
      return this.pct.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(OUT, patientCompartments, DEAD_LETTER, failedReads);
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
                .withOutputTags(OUT, TupleTagList.of(DEAD_LETTER)));
    return new Result(results);
  }

  /** GetPatientEverythingFn executes a GetPatientEverything request. */
  static class GetPatientEverythingFn extends DoFn<PatientEverythingParameter, JsonArray> {

    private static final Counter GET_PATIENT_EVERYTHING_ERROR_COUNT =
        Metrics.counter(
            GetPatientEverythingFn.class,
            BASE_METRIC_PREFIX + "get_patient_everything_error_count");
    private static final Counter GET_PATIENT_EVERYTHING_SUCCESS_COUNT =
        Metrics.counter(
            GetPatientEverythingFn.class,
            BASE_METRIC_PREFIX + "get_patient_everything_success_count");
    private static final Distribution GET_PATIENT_EVERYTHING_LATENCY_MS =
        Metrics.distribution(
            GetPatientEverythingFn.class, BASE_METRIC_PREFIX + "get_patient_everything_latency_ms");
    private static final Logger LOG = LoggerFactory.getLogger(GetPatientEverythingFn.class);

    @SuppressWarnings("initialization.field.uninitialized")
    private HealthcareApiClient client;

    /**
     * Instantiate healthcare client (version v1).
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
        context.output(
            getPatientEverything(
                patientEverythingParameter.getResourceName(),
                patientEverythingParameter.getFilters()));
      } catch (IllegalArgumentException | NoSuchElementException e) {
        GET_PATIENT_EVERYTHING_ERROR_COUNT.inc();
        LOG.warn(
            String.format(
                "Error executing GetPatientEverything: FHIR resources writing to Dead Letter "
                    + "Queue. Cause: %s Stack Trace: %s",
                e.getMessage(), Throwables.getStackTraceAsString(e)));
        context.output(DEAD_LETTER, HealthcareIOError.of(patientEverythingParameter.toString(), e));
      }
    }

    private JsonArray getPatientEverything(
        String resourceName, @Nullable Map<String, String> filters) {
      long start = Instant.now().toEpochMilli();

      HashMap<String, Object> filterObjects = new HashMap<>();
      if (filters != null) {
        filterObjects.putAll(filters);
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
