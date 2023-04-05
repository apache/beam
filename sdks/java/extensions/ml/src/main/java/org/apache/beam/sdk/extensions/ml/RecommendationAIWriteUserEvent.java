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
import com.google.cloud.recommendationengine.v1beta1.EventStoreName;
import com.google.cloud.recommendationengine.v1beta1.UserEvent;
import com.google.cloud.recommendationengine.v1beta1.UserEventServiceClient;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
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
 * Takes an input {@link PCollection} of {@link GenericJson}s and converts them to and creates
 * {@link UserEvent}s.
 *
 * <p>It is possible to provide a catalog name to which you want to add the user event (defaults to
 * "default_catalog"). It is possible to provide a event store to which you want to add the user
 * event (defaults to "default_event_store").
 */
@AutoValue
@SuppressWarnings({"nullness"})
public abstract class RecommendationAIWriteUserEvent
    extends PTransform<PCollection<GenericJson>, PCollectionTuple> {

  public static final TupleTag<UserEvent> SUCCESS_TAG = new TupleTag<UserEvent>() {};
  public static final TupleTag<UserEvent> FAILURE_TAG = new TupleTag<UserEvent>() {};

  static Builder newBuilder() {
    return new AutoValue_RecommendationAIWriteUserEvent.Builder()
        .setCatalogName("default_catalog")
        .setEventStore("default_event_store");
  }

  /** @return ID of Google Cloud project to be used for creating user events. */
  public abstract @Nullable String projectId();

  /** @return Name of the catalog where the user events will be created. */
  public abstract @Nullable String catalogName();

  /** @return Name of the event store where the user events will be created. */
  public abstract @Nullable String eventStore();

  /**
   * The transform converts the contents of input PCollection into {@link UserEvent}s and then calls
   * the Recommendation AI service to create the user event.
   *
   * @param input input PCollection
   * @return PCollectionTuple with successful and failed {@link UserEvent}s
   */
  @Override
  public PCollectionTuple expand(PCollection<GenericJson> input) {
    return input.apply(
        ParDo.of(new WriteUserEvent(projectId(), catalogName(), eventStore()))
            .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
  }

  @AutoValue.Builder
  abstract static class Builder {
    /** @param projectId ID of Google Cloud project to be used for creating user events. */
    public abstract Builder setProjectId(@Nullable String projectId);

    /** @param catalogName Name of the catalog where the user events will be created. */
    public abstract Builder setCatalogName(@Nullable String catalogName);

    /** @param eventStore Name of the event store where the user events will be created. */
    public abstract Builder setEventStore(@Nullable String eventStore);

    public abstract RecommendationAIWriteUserEvent build();
  }

  abstract Builder toBuilder();

  public RecommendationAIWriteUserEvent withProjectId(String projectId) {
    return this.toBuilder().setProjectId(projectId).build();
  }

  public RecommendationAIWriteUserEvent withCatalogName(String catalogName) {
    return this.toBuilder().setCatalogName(catalogName).build();
  }

  public RecommendationAIWriteUserEvent withEventStore(String eventStore) {
    return this.toBuilder().setEventStore(eventStore).build();
  }

  private static class WriteUserEvent extends DoFn<GenericJson, UserEvent> {
    private final String projectId;
    private final String catalogName;
    private final String eventStore;

    /**
     * @param projectId ID of GCP project to be used for creating user events.
     * @param catalogName Catalog name for UserEvent creation.
     * @param eventStore Event store for UserEvent creation.
     */
    private WriteUserEvent(String projectId, String catalogName, String eventStore) {
      this.projectId = projectId;
      this.catalogName = catalogName;
      this.eventStore = eventStore;
    }

    @ProcessElement
    public void processElement(ProcessContext context)
        throws IOException, ExecutionException, InterruptedException {
      EventStoreName parent = EventStoreName.of(projectId, "global", catalogName, eventStore);
      UserEvent.Builder userEventBuilder = UserEvent.newBuilder();
      JsonFormat.parser().merge(new JSONObject(context.element()).toString(), userEventBuilder);
      UserEvent userEvent = userEventBuilder.build();

      try (UserEventServiceClient userEventServiceClient = UserEventServiceClient.create()) {
        UserEvent response = userEventServiceClient.writeUserEvent(parent, userEvent);

        context.output(SUCCESS_TAG, response);
      } catch (ApiException e) {
        context.output(FAILURE_TAG, userEvent);
      }
    }
  }
}
