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

import com.google.gson.JsonArray;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOSearch.SearchParameter;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.FhirResourcePages.FhirResourcePagesIterator;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The type Search. */
public class FhirIOSearch<T>
    extends PTransform<PCollection<SearchParameter<T>>, FhirIOSearch.Result> {

  private final ValueProvider<String> fhirStore;

  FhirIOSearch(ValueProvider<String> fhirStore) {
    this.fhirStore = fhirStore;
  }

  FhirIOSearch(String fhirStore) {
    this.fhirStore = StaticValueProvider.of(fhirStore);
  }

  public static class Result implements POutput, PInput {

    private final PCollection<KV<String, JsonArray>> keyedResources;
    private final PCollection<JsonArray> resources;

    private final PCollection<HealthcareIOError<String>> failedSearches;
    PCollectionTuple pct;

    /**
     * Create FhirIO.Search.Result form PCollectionTuple with OUT and DEAD_LETTER tags.
     *
     * @param pct the pct
     * @return the search result
     * @throws IllegalArgumentException the illegal argument exception
     */
    static Result of(PCollectionTuple pct) throws IllegalArgumentException {
      if (pct.has(OUT) && pct.has(DEAD_LETTER)) {
        return new Result(pct);
      } else {
        throw new IllegalArgumentException(
            "The PCollection tuple must have the FhirIO.Search.OUT "
                + "and FhirIO.Search.DEAD_LETTER tuple tags");
      }
    }

    private Result(PCollectionTuple pct) {
      this.pct = pct;
      this.keyedResources =
          pct.get(OUT).setCoder(KvCoder.of(StringUtf8Coder.of(), JsonArrayCoder.of()));
      this.resources =
          this.keyedResources
              .apply(
                  "Extract Values",
                  MapElements.into(TypeDescriptor.of(JsonArray.class)).via(KV::getValue))
              .setCoder(JsonArrayCoder.of());
      this.failedSearches =
          pct.get(DEAD_LETTER).setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
    }

    /**
     * Gets failed searches.
     *
     * @return the failed searches
     */
    public PCollection<HealthcareIOError<String>> getFailedSearches() {
      return failedSearches;
    }

    /**
     * Gets resources.
     *
     * @return the resources
     */
    public PCollection<JsonArray> getResources() {
      return resources;
    }

    /**
     * Gets resources with input SearchParameter key.
     *
     * @return the resources with input SearchParameter key.
     */
    public PCollection<KV<String, JsonArray>> getKeyedResources() {
      return keyedResources;
    }

    @Override
    public Pipeline getPipeline() {
      return this.pct.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(OUT, keyedResources);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  /** The tag for the main output of FHIR resources. */
  public static final TupleTag<KV<String, JsonArray>> OUT =
      new TupleTag<KV<String, JsonArray>>() {};
  /** The tag for the deadletter output of FHIR resources. */
  public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
      new TupleTag<HealthcareIOError<String>>() {};

  @Override
  public Result expand(PCollection<SearchParameter<T>> input) {
    return input.apply("Search FHIR Resources", new SearchResourcesJsonString(this.fhirStore));
  }

  /**
   * DoFn to read resources from an Google Cloud Healthcare FHIR store based on search request
   *
   * <p>This DoFn consumes a {@link PCollection} of search requests consisting of resource type and
   * search parameters, and fetches all matching resources based on the search criteria and will
   * output a {@link PCollectionTuple} which contains the output and dead-letter {@link
   * PCollection}*.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link FhirIOSearch#OUT} - Contains all {@link PCollection} records successfully search
   *       from the Fhir store.
   *   <li>{@link FhirIOSearch#DEAD_LETTER} - Contains all {@link PCollection} of {@link
   *       HealthcareIOError}* of failed searches from the Fhir store, with error message and
   *       stacktrace.
   * </ul>
   */
  class SearchResourcesJsonString extends PTransform<PCollection<SearchParameter<T>>, Result> {

    private final ValueProvider<String> fhirStore;

    public SearchResourcesJsonString(ValueProvider<String> fhirStore) {
      this.fhirStore = fhirStore;
    }

    @Override
    public Result expand(PCollection<SearchParameter<T>> resourceIds) {
      return new Result(
          resourceIds.apply(
              ParDo.of(new SearchResourcesFn(this.fhirStore))
                  .withOutputTags(OUT, TupleTagList.of(DEAD_LETTER))));
    }

    /** DoFn for searching resources from the FHIR store with error handling. */
    class SearchResourcesFn extends DoFn<SearchParameter<T>, KV<String, JsonArray>> {

      private final Counter searchResourceErrors =
          Metrics.counter(
              SearchResourcesFn.class, FhirIO.BASE_METRIC_PREFIX + "search_resource_error_count");
      private final Counter searchResourceSuccess =
          Metrics.counter(
              SearchResourcesFn.class, FhirIO.BASE_METRIC_PREFIX + "search_resource_success_count");
      private final Distribution searchResourceLatencyMs =
          Metrics.distribution(
              SearchResourcesFn.class, FhirIO.BASE_METRIC_PREFIX + "search_resource_latency_ms");

      private final Logger LOG = LoggerFactory.getLogger(SearchResourcesFn.class);
      private HealthcareApiClient client;
      private final ValueProvider<String> fhirStore;

      /** Instantiates a new Fhir resources search fn. */
      SearchResourcesFn(ValueProvider<String> fhirStore) {
        this.fhirStore = fhirStore;
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
        SearchParameter<T> fhirSearchParameters = context.element();
        try {
          context.output(
              KV.of(
                  fhirSearchParameters.getKey(),
                  searchResources(
                      this.client,
                      this.fhirStore.toString(),
                      fhirSearchParameters.getResourceType(),
                      fhirSearchParameters.getQueries())));
        } catch (IllegalArgumentException | NoSuchElementException e) {
          searchResourceErrors.inc();
          LOG.warn(
              String.format(
                  "Error search FHIR resources writing to Dead Letter "
                      + "Queue. Cause: %s Stack Trace: %s",
                  e.getMessage(), Throwables.getStackTraceAsString(e)));
          context.output(DEAD_LETTER, HealthcareIOError.of(this.fhirStore.toString(), e));
        }
      }

      private JsonArray searchResources(
          HealthcareApiClient client,
          String fhirStore,
          String resourceType,
          @Nullable Map<String, T> parameters)
          throws NoSuchElementException {
        long start = Instant.now().toEpochMilli();

        HashMap<String, Object> parameterObjects = new HashMap<>();
        if (parameters != null) {
          parameters.forEach(parameterObjects::put);
        }
        FhirResourcePagesIterator iter =
            new FhirResourcePagesIterator(client, fhirStore, resourceType, parameterObjects);
        JsonArray result = new JsonArray();
        while (iter.hasNext()) {
          result.addAll(iter.next());
        }
        searchResourceLatencyMs.update(Instant.now().toEpochMilli() - start);
        searchResourceSuccess.inc();
        return result;
      }
    }
  }

  /**
   * FhirSearchParameter represents the query parameters for a FHIR search request, used as a
   * parameter for {@link FhirIOSearch}.
   */
  @DefaultCoder(SearchParameterCoder.class)
  public static class SearchParameter<T> {

    private final String resourceType;
    // The key is used as a key for the search query, if there is source information to propagate
    // through the pipeline.
    private final String key;
    private final @Nullable Map<String, T> queries;

    private SearchParameter(
        String resourceType, @Nullable String key, @Nullable Map<String, T> queries) {
      this.resourceType = resourceType;
      if (key != null) {
        this.key = key;
      } else {
        this.key = "";
      }
      this.queries = queries;
    }

    public static <T> SearchParameter<T> of(
        String resourceType, @Nullable String key, @Nullable Map<String, T> queries) {
      return new SearchParameter<>(resourceType, key, queries);
    }

    public static <T> SearchParameter<T> of(String resourceType, @Nullable Map<String, T> queries) {
      return new SearchParameter<>(resourceType, null, queries);
    }

    public String getResourceType() {
      return resourceType;
    }

    public String getKey() {
      return key;
    }

    public @Nullable Map<String, T> getQueries() {
      return queries;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SearchParameter<?> that = (SearchParameter<?>) o;
      return Objects.equals(resourceType, that.resourceType)
          && Objects.equals(key, that.key)
          && Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resourceType, queries);
    }

    @Override
    public String toString() {
      return String.format(
          "FhirSearchParameter{resourceType='%s', key='%s', queries='%s'}",
          resourceType, key, queries);
    }
  }

  /**
   * SearchParameterCoder is the coder for {@link SearchParameter}, which takes a coder for type T.
   */
  public static class SearchParameterCoder<T> extends CustomCoder<SearchParameter<T>> {

    private static final NullableCoder<String> STRING_CODER =
        NullableCoder.of(StringUtf8Coder.of());
    private final NullableCoder<Map<String, T>> originalCoder;

    SearchParameterCoder(Coder<T> originalCoder) {
      this.originalCoder = NullableCoder.of(MapCoder.of(STRING_CODER, originalCoder));
    }

    public static <T> SearchParameterCoder<T> of(Coder<T> originalCoder) {
      return new SearchParameterCoder<T>(originalCoder);
    }

    @Override
    public void encode(SearchParameter<T> value, OutputStream outStream) throws IOException {
      STRING_CODER.encode(value.getResourceType(), outStream);
      STRING_CODER.encode(value.getKey(), outStream);
      originalCoder.encode(value.getQueries(), outStream);
    }

    @Override
    public SearchParameter<T> decode(InputStream inStream) throws IOException {
      String resourceType = STRING_CODER.decode(inStream);
      String key = STRING_CODER.decode(inStream);
      Map<String, T> queries = originalCoder.decode(inStream);
      return SearchParameter.of(resourceType, key, queries);
    }
  }
}
