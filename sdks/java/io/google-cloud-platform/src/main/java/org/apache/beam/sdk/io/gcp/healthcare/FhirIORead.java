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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.healthcare.v1.model.HttpBody;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The type Read. */
public class FhirIORead extends PTransform<PCollection<String>, FhirIORead.Result> {

  /** Instantiates a new Read. */
  public FhirIORead() {}

  /** The type Result. */
  public static class Result implements POutput, PInput {

    private PCollection<String> resources;

    private PCollection<HealthcareIOError<String>> failedReads;
    /** The Pct. */
    PCollectionTuple pct;

    /**
     * Create FhirIO.Read.Result form PCollectionTuple with OUT and DEAD_LETTER tags.
     *
     * @param pct the pct
     * @return the read result
     * @throws IllegalArgumentException the illegal argument exception
     */
    static Result of(PCollectionTuple pct) throws IllegalArgumentException {
      if (pct.has(OUT) && pct.has(DEAD_LETTER)) {
        return new Result(pct);
      } else {
        throw new IllegalArgumentException(
            "The PCollection tuple must have the FhirIO.Read.OUT "
                + "and FhirIO.Read.DEAD_LETTER tuple tags");
      }
    }

    private Result(PCollectionTuple pct) {
      this.pct = pct;
      this.resources = pct.get(OUT);
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
     * Gets resources.
     *
     * @return the resources
     */
    public PCollection<String> getResources() {
      return resources;
    }

    @Override
    public Pipeline getPipeline() {
      return this.pct.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(OUT, resources);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  /** The tag for the main output of FHIR Resources. */
  public static final TupleTag<String> OUT = new TupleTag<String>() {};
  /** The tag for the deadletter output of FHIR Resources. */
  public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
      new TupleTag<HealthcareIOError<String>>() {};

  @Override
  public Result expand(PCollection<String> input) {
    return input.apply("Get FHIR Resources", new GetResourceJsonString());
  }

  /**
   * DoFn to fetch a resource from an Google Cloud Healthcare FHIR store based on resourceID
   *
   * <p>This DoFn consumes a {@link PCollection} of notifications {@link String}s from the FHIR
   * store, and fetches the actual {@link String} object based on the id in the notification and
   * will output a {@link PCollectionTuple} which contains the output and dead-letter {@link
   * PCollection}*.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link FhirIORead#OUT} - Contains all {@link PCollection} records successfully read from
   *       the Fhir store.
   *   <li>{@link FhirIORead#DEAD_LETTER} - Contains all {@link PCollection} of {@link
   *       HealthcareIOError}* of resource IDs which failed to be fetched from the Fhir store, with
   *       error message and stacktrace.
   * </ul>
   */
  static class GetResourceJsonString extends PTransform<PCollection<String>, Result> {

    /** Instantiates a new Get FHIR Resource DoFn. */
    public GetResourceJsonString() {}

    @Override
    public Result expand(PCollection<String> resourceIds) {
      return new Result(
          resourceIds.apply(
              ParDo.of(new GetResourceFn()).withOutputTags(OUT, TupleTagList.of(DEAD_LETTER))));
    }

    /** DoFn for getting resources from the Fhir store with error handling. */
    static class GetResourceFn extends DoFn<String, String> {

      private static final Logger LOG = LoggerFactory.getLogger(GetResourceFn.class);
      private static final Counter READ_RESOURCE_ERRORS =
          Metrics.counter(
              GetResourceFn.class, FhirIO.BASE_METRIC_PREFIX + "read_resource_error_count");
      private static final Counter READ_RESOURCE_SUCCESS =
          Metrics.counter(
              GetResourceFn.class, FhirIO.BASE_METRIC_PREFIX + "read_resource_success_count");
      private static final Distribution READ_RESOURCE_LATENCY_MS =
          Metrics.distribution(
              GetResourceFn.class, FhirIO.BASE_METRIC_PREFIX + "read_resource_latency_ms");

      private final ObjectMapper mapper;

      @SuppressWarnings("initialization.fields.uninitialized")
      private HealthcareApiClient client;

      /** Instantiates a new FHIR GetResource fn. */
      GetResourceFn() {
        this.mapper = new ObjectMapper();
      }

      /**
       * Instantiate healthcare client.
       *
       * @throws IOException the io exception
       */
      @Setup
      public void instantiateHealthcareClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      /**
       * Process element.
       *
       * @param context the context
       */
      @ProcessElement
      public void processElement(ProcessContext context) {
        String resourceId = context.element();
        try {
          context.output(fetchResource(this.client, resourceId));
        } catch (Exception e) {
          READ_RESOURCE_ERRORS.inc();
          LOG.warn(
              String.format(
                  "Error fetching FHIR resource with ID %s writing to Dead Letter "
                      + "Queue. Cause: %s Stack Trace: %s",
                  resourceId, e.getMessage(), Throwables.getStackTraceAsString(e)));
          context.output(DEAD_LETTER, HealthcareIOError.of(resourceId, e));
        }
      }

      private String fetchResource(HealthcareApiClient client, String resourceId)
          throws IOException, IllegalArgumentException {
        long startTime = Instant.now().toEpochMilli();

        HttpBody resource = client.readFhirResource(resourceId);
        READ_RESOURCE_LATENCY_MS.update(Instant.now().toEpochMilli() - startTime);

        if (resource == null) {
          throw new IOException(String.format("GET request for %s returned null", resourceId));
        }
        READ_RESOURCE_SUCCESS.inc();
        return mapper.writeValueAsString(resource);
      }
    }
  }
}
