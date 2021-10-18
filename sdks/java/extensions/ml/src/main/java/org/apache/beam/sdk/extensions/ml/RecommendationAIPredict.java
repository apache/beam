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
package org.apache.beam.sdk.extensions.ml;

import com.google.api.client.json.GenericJson;
import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.recommendationengine.v1beta1.PlacementName;
import com.google.cloud.recommendationengine.v1beta1.PredictResponse;
import com.google.cloud.recommendationengine.v1beta1.PredictionServiceClient;
import com.google.cloud.recommendationengine.v1beta1.UserEvent;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.json.JSONObject;

/**
 * A {@link PTransform} using the Recommendations AI API (https://cloud.google.com/recommendations).
 * Takes an input {@link PCollection} of {@link GenericJson}s and creates {@link
 * PredictResponse.PredictionResult}s.
 *
 * <p>It is possible to provide a catalog name to which you want to add the user event (defaults to
 * "default_catalog"). It is possible to provide a event store to which you want to add the user
 * event (defaults to "default_event_store"). A placement id for the recommendation engine placement
 * to be used.
 */
@AutoValue
@SuppressWarnings({"nullness"})
public abstract class RecommendationAIPredict
    extends PTransform<PCollection<GenericJson>, PCollectionTuple> {

  /** @return ID of Google Cloud project to be used for creating catalog items. */
  public abstract @Nullable String projectId();

  /** @return Name of the catalog where the catalog items will be created. */
  public abstract @Nullable String catalogName();

  /** @return Name of the event store where the user events will be created. */
  public abstract @Nullable String eventStore();

  /** @return ID of the recommendation engine placement. */
  public abstract String placementId();

  public static final TupleTag<PredictResponse.PredictionResult> SUCCESS_TAG =
      new TupleTag<PredictResponse.PredictionResult>() {};

  public static final TupleTag<UserEvent> FAILURE_TAG = new TupleTag<UserEvent>() {};

  @AutoValue.Builder
  abstract static class Builder {
    /** @param projectId ID of Google Cloud project to be used for the predictions. */
    public abstract Builder setProjectId(@Nullable String projectId);

    /** @param catalogName Name of the catalog to be used for predictions. */
    public abstract Builder setCatalogName(@Nullable String catalogName);

    /** @param eventStore Name of the event store to be used for predictions. */
    public abstract Builder setEventStore(@Nullable String eventStore);

    /** @param placementId of the recommendation engine placement. */
    public abstract Builder setPlacementId(String placementId);

    public abstract RecommendationAIPredict build();
  }

  static Builder newBuilder() {
    return new AutoValue_RecommendationAIPredict.Builder()
        .setCatalogName("default_catalog")
        .setEventStore("default_event_store")
        .setPlacementId("recently_viewed_default");
  }

  abstract Builder toBuilder();

  public RecommendationAIPredict withProjectId(String projectId) {
    return this.toBuilder().setProjectId(projectId).build();
  }

  public RecommendationAIPredict withCatalogName(String catalogName) {
    return this.toBuilder().setCatalogName(catalogName).build();
  }

  public RecommendationAIPredict withEventStore(String eventStore) {
    return this.toBuilder().setEventStore(eventStore).build();
  }

  public RecommendationAIPredict withPlacementId(String placementId) {
    return this.toBuilder().setPlacementId(placementId).build();
  }

  @Override
  public PCollectionTuple expand(PCollection<GenericJson> input) {
    return input.apply(
        ParDo.of(new Predict(projectId(), catalogName(), eventStore(), placementId()))
            .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
  }

  private static class Predict extends DoFn<GenericJson, PredictResponse.PredictionResult> {
    private final String projectId;
    private final String catalogName;
    private final String eventStore;
    private final String placementId;

    /**
     * @param projectId ID of GCP project to be used for creating catalog items.
     * @param catalogName Catalog name for UserEvent creation.
     * @param eventStore Event store for UserEvent creation.
     * @param placementId ID of the recommendation engine placement.
     */
    private Predict(String projectId, String catalogName, String eventStore, String placementId) {
      this.projectId = projectId;
      this.catalogName = catalogName;
      this.eventStore = eventStore;
      this.placementId = placementId;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      PlacementName name =
          PlacementName.of(projectId, "global", catalogName, eventStore, placementId);
      UserEvent.Builder userEventBuilder = UserEvent.newBuilder();
      JsonFormat.parser().merge(new JSONObject(context.element()).toString(), userEventBuilder);
      UserEvent userEvent = userEventBuilder.build();
      try (PredictionServiceClient predictionServiceClient = PredictionServiceClient.create()) {
        for (PredictResponse.PredictionResult res :
            predictionServiceClient.predict(name, userEvent).iterateAll()) {
          context.output(SUCCESS_TAG, res);
        }
      } catch (ApiException e) {
        context.output(FAILURE_TAG, userEvent);
      }
    }
  }
}
