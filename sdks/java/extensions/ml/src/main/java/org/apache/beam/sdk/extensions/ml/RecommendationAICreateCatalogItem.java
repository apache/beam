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
import com.google.cloud.recommendationengine.v1beta1.CatalogItem;
import com.google.cloud.recommendationengine.v1beta1.CatalogName;
import com.google.cloud.recommendationengine.v1beta1.CatalogServiceClient;
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
 * Takes an input {@link PCollection} of {@link GenericJson}s and converts them to and creates
 * {@link CatalogItem}s. It outputs a PCollectionTuple which will contain the successfully created
 * and failed catalog items.
 *
 * <p>It is possible to provide a catalog name to which you want to add the catalog item (defaults
 * to "default_catalog").
 */
@AutoValue
@SuppressWarnings({"nullness"})
public abstract class RecommendationAICreateCatalogItem
    extends PTransform<PCollection<GenericJson>, PCollectionTuple> {

  /** @return ID of Google Cloud project to be used for creating catalog items. */
  public abstract @Nullable String projectId();

  /**
   * @return Name of the catalog where the catalog items will be created (defaults to
   *     "default_catalog").
   */
  public abstract @Nullable String catalogName();

  public static final TupleTag<CatalogItem> SUCCESS_TAG = new TupleTag<CatalogItem>() {};

  public static final TupleTag<CatalogItem> FAILURE_TAG = new TupleTag<CatalogItem>() {};

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    /** @param projectId ID of Google Cloud project to be used for creating catalog items. */
    public abstract Builder setProjectId(@Nullable String projectId);

    /** @param catalogName Name of the catalog where the catalog items will be created. */
    public abstract Builder setCatalogName(@Nullable String catalogName);

    public abstract RecommendationAICreateCatalogItem build();
  }

  static Builder newBuilder() {
    return new AutoValue_RecommendationAICreateCatalogItem.Builder()
        .setCatalogName("default_catalog");
  }

  public RecommendationAICreateCatalogItem withProjectId(String projectId) {
    return this.toBuilder().setProjectId(projectId).build();
  }

  public RecommendationAICreateCatalogItem withCatalogName(String catalogName) {
    return this.toBuilder().setCatalogName(catalogName).build();
  }

  /**
   * The transform converts the contents of input PCollection into {@link CatalogItem}s and then
   * calls the Recommendation AI service to create the catalog item.
   *
   * @param input input PCollection
   * @return PCollectionTuple with successful and failed {@link CatalogItem}s
   */
  @Override
  public PCollectionTuple expand(PCollection<GenericJson> input) {
    return input.apply(
        ParDo.of(new CreateCatalogItem(projectId(), catalogName()))
            .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
  }

  private static class CreateCatalogItem extends DoFn<GenericJson, CatalogItem> {
    private final String projectId;
    private final String catalogName;

    /**
     * @param projectId ID of GCP project to be used for creating catalog items.
     * @param catalogName Catalog name for CatalogItem creation.
     */
    private CreateCatalogItem(String projectId, String catalogName) {
      this.projectId = projectId;
      this.catalogName = catalogName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      CatalogName parent = CatalogName.of(projectId, "global", catalogName);
      CatalogItem.Builder catalogItemBuilder = CatalogItem.newBuilder();
      JsonFormat.parser().merge(new JSONObject(context.element()).toString(), catalogItemBuilder);
      CatalogItem catalogItem = catalogItemBuilder.build();

      try (CatalogServiceClient catalogServiceClient = CatalogServiceClient.create()) {
        CatalogItem response = catalogServiceClient.createCatalogItem(parent, catalogItem);

        context.output(SUCCESS_TAG, response);
      } catch (ApiException e) {
        context.output(FAILURE_TAG, catalogItem);
      }
    }
  }
}
