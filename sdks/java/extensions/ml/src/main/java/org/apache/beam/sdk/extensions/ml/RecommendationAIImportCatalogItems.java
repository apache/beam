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
import com.google.cloud.recommendationengine.v1beta1.CatalogInlineSource;
import com.google.cloud.recommendationengine.v1beta1.CatalogItem;
import com.google.cloud.recommendationengine.v1beta1.CatalogName;
import com.google.cloud.recommendationengine.v1beta1.CatalogServiceClient;
import com.google.cloud.recommendationengine.v1beta1.ImportCatalogItemsRequest;
import com.google.cloud.recommendationengine.v1beta1.ImportCatalogItemsResponse;
import com.google.cloud.recommendationengine.v1beta1.InputConfig;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.json.JSONObject;

/**
 * A {@link PTransform} connecting to the Recommendations AI API
 * (https://cloud.google.com/recommendations) and creating {@link CatalogItem}s. *
 *
 * <p>Batch size defines how many items are created at once per batch (max: 5000).
 *
 * <p>The transform consumes {@link KV} of {@link String} and {@link GenericJson}s (assumed to be
 * the catalog item id as key and contents as value) and outputs a PCollectionTuple which will
 * contain the successfully created and failed catalog items.
 *
 * <p>It is possible to provide a catalog name to which you want to add the catalog item (defaults
 * to "default_catalog").
 */
@AutoValue
@SuppressWarnings({"nullness"})
public abstract class RecommendationAIImportCatalogItems
    extends PTransform<PCollection<KV<String, GenericJson>>, PCollectionTuple> {

  public static final TupleTag<CatalogItem> SUCCESS_TAG = new TupleTag<CatalogItem>() {};
  public static final TupleTag<CatalogItem> FAILURE_TAG = new TupleTag<CatalogItem>() {};

  static Builder newBuilder() {
    return new AutoValue_RecommendationAIImportCatalogItems.Builder();
  }

  abstract Builder toBuilder();

  /** @return ID of Google Cloud project to be used for creating catalog items. */
  public abstract @Nullable String projectId();

  /** @return Name of the catalog where the catalog items will be created. */
  public abstract @Nullable String catalogName();

  /** @return Size of input elements batch to be sent in one request. */
  public abstract Integer batchSize();

  /**
   * @return Time limit (in processing time) on how long an incomplete batch of elements is allowed
   *     to be buffered.
   */
  public abstract Duration maxBufferingDuration();

  public RecommendationAIImportCatalogItems withProjectId(String projectId) {
    return this.toBuilder().setProjectId(projectId).build();
  }

  public RecommendationAIImportCatalogItems withCatalogName(String catalogName) {
    return this.toBuilder().setCatalogName(catalogName).build();
  }

  public RecommendationAIImportCatalogItems withBatchSize(Integer batchSize) {
    return this.toBuilder().setBatchSize(batchSize).build();
  }

  /**
   * The transform converts the contents of input PCollection into {@link CatalogItem}s and then
   * calls the Recommendation AI service to create the catalog item.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollectionTuple expand(PCollection<KV<String, GenericJson>> input) {
    return input
        .apply(
            "Batch Contents",
            GroupIntoBatches.<String, GenericJson>ofSize(batchSize())
                .withMaxBufferingDuration(maxBufferingDuration())
                .withShardedKey())
        .apply(
            "Import CatalogItems",
            ParDo.of(new ImportCatalogItems(projectId(), catalogName()))
                .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));
  }

  @AutoValue.Builder
  abstract static class Builder {
    /** @param projectId ID of Google Cloud project to be used for creating catalog items. */
    public abstract Builder setProjectId(@Nullable String projectId);

    /** @param catalogName Name of the catalog where the catalog items will be created. */
    public abstract Builder setCatalogName(@Nullable String catalogName);

    /**
     * @param batchSize Amount of input elements to be sent to Recommendation AI service in one
     *     request.
     */
    public abstract Builder setBatchSize(Integer batchSize);

    /**
     * @param maxBufferingDuration Time limit (in processing time) on how long an incomplete batch
     *     of elements is allowed to be buffered.
     */
    public abstract Builder setMaxBufferingDuration(Duration maxBufferingDuration);

    public abstract RecommendationAIImportCatalogItems build();
  }

  private static class ImportCatalogItems
      extends DoFn<KV<ShardedKey<String>, Iterable<GenericJson>>, CatalogItem> {
    private final String projectId;
    private final String catalogName;

    /**
     * @param projectId ID of GCP project to be used for creating catalog items.
     * @param catalogName Catalog name for CatalogItem creation.
     */
    private ImportCatalogItems(String projectId, String catalogName) {
      this.projectId = projectId;
      this.catalogName = catalogName;
    }

    @ProcessElement
    public void processElement(ProcessContext c)
        throws IOException, ExecutionException, InterruptedException {
      CatalogName parent = CatalogName.of(projectId, "global", catalogName);

      ArrayList<CatalogItem> catalogItems = new ArrayList<>();
      for (GenericJson element : c.element().getValue()) {
        CatalogItem.Builder catalogItemBuilder = CatalogItem.newBuilder();
        JsonFormat.parser().merge(new JSONObject(element).toString(), catalogItemBuilder);
        catalogItems.add(catalogItemBuilder.build());
      }
      CatalogInlineSource catalogInlineSource =
          CatalogInlineSource.newBuilder().addAllCatalogItems(catalogItems).build();

      InputConfig inputConfig =
          InputConfig.newBuilder().mergeCatalogInlineSource(catalogInlineSource).build();
      ImportCatalogItemsRequest request =
          ImportCatalogItemsRequest.newBuilder()
              .setParent(parent.toString())
              .setInputConfig(inputConfig)
              .build();
      try (CatalogServiceClient catalogServiceClient = CatalogServiceClient.create()) {
        ImportCatalogItemsResponse response =
            catalogServiceClient.importCatalogItemsAsync(request).get();
        if (response.getErrorSamplesCount() > 0) {
          for (CatalogItem ci : catalogItems) {
            c.output(FAILURE_TAG, ci);
          }
        } else {
          for (CatalogItem ci : catalogItems) {
            c.output(SUCCESS_TAG, ci);
          }
        }
      } catch (ApiException e) {
        for (CatalogItem ci : catalogItems) {
          c.output(SUCCESS_TAG, ci);
        }
      }
    }
  }
}
