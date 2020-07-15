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

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.ReidentifyContentRequest;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link PTransform} connecting to Cloud DLP (https://cloud.google.com/dlp/docs/libraries) and
 * inspecting text for identifying data according to provided settings.
 *
 * <p>The transform supports both delimited columnar input data and unstructured input.
 *
 * <p>If the headerColumns property is set and a sideinput with headers is added to the PTransform,
 * delimiter also should be set, else the results will be incorrect. If headerColumns is neither set
 * nor passed as sideinput, input is assumed to be unstructured.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>The transform consumes {@link KV} of {@link String}s (assumed to be filename as key and
 * contents as value) and outputs {@link KV} of {@link String} (eg. filename) and {@link
 * ReidentifyContentResponse}, which will contain {@link Table} of results for the user to consume.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>Either reidentifyTemplateName {@link String} or reidentifyConfig {@link DeidentifyConfig} need
 * to be set. inspectConfig {@link InspectConfig} and inspectTemplateName {@link String} are
 * optional.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 */
@Experimental
@AutoValue
public abstract class DLPReidentifyText
    extends PTransform<
        PCollection<KV<String, String>>, PCollection<KV<String, ReidentifyContentResponse>>> {

  public static final Integer DLP_PAYLOAD_LIMIT_BYTES = 524000;

  /** @return Template name for data inspection. */
  @Nullable
  public abstract String getInspectTemplateName();

  /** @return Template name for data reidentification. */
  @Nullable
  public abstract String getReidentifyTemplateName();

  /**
   * @return Configuration object for data inspection. If present, supersedes the template settings.
   */
  @Nullable
  public abstract InspectConfig getInspectConfig();

  /** @return Configuration object for reidentification. If present, supersedes the template. */
  @Nullable
  public abstract DeidentifyConfig getReidentifyConfig();

  /** @return Delimiter to be used when splitting values from input strings into columns. */
  @Nullable
  public abstract String getColumnDelimiter();

  /** @return List of column names if the input KV value is a delimited row. */
  @Nullable
  public abstract PCollectionView<List<String>> getHeaderColumns();

  /** @return Size of input elements batch to be sent to Cloud DLP service in one request. */
  public abstract Integer getBatchSizeBytes();

  /** @return ID of Google Cloud project to be used when deidentifying data. */
  public abstract String getProjectId();

  @AutoValue.Builder
  public abstract static class Builder {
    /** @param inspectTemplateName Template name for data inspection. */
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    /**
     * @param inspectConfig Configuration object for data inspection. If present, supersedes the
     *     template settings.
     */
    public abstract Builder setInspectConfig(InspectConfig inspectConfig);

    /**
     * @param reidentifyConfig Configuration object for data deidentification. If present,
     *     supersedes the template settings.
     */
    public abstract Builder setReidentifyConfig(DeidentifyConfig reidentifyConfig);

    /** @param reidentifyTemplateName Template name for data deidentification. */
    public abstract Builder setReidentifyTemplateName(String reidentifyTemplateName);

    /**
     * @param batchSize Size of input elements batch to be sent to Cloud DLP service in one request.
     */
    public abstract Builder setBatchSizeBytes(Integer batchSize);
    /** @param headerColumns List of column names if the input KV value is a delimited row. */
    public abstract Builder setHeaderColumns(PCollectionView<List<String>> headerColumns);

    /**
     * @param delimiter Delimiter to be used when splitting values from input strings into columns.
     */
    public abstract Builder setColumnDelimiter(String delimiter);

    /** @param projectId ID of Google Cloud project to be used when deidentifying data. */
    public abstract Builder setProjectId(String projectId);

    abstract DLPReidentifyText autoBuild();

    public DLPReidentifyText build() {
      DLPReidentifyText dlpReidentifyText = autoBuild();
      if (dlpReidentifyText.getReidentifyConfig() == null
          && dlpReidentifyText.getReidentifyTemplateName() == null) {
        throw new IllegalArgumentException(
            "Either reidentifyConfig or reidentifyTemplateName need to be set!");
      }
      if (dlpReidentifyText.getBatchSizeBytes() > DLP_PAYLOAD_LIMIT_BYTES) {
        throw new IllegalArgumentException(
            String.format(
                "Batch size is too large! It should be smaller or equal than %d.",
                DLP_PAYLOAD_LIMIT_BYTES));
      }
      if (dlpReidentifyText.getColumnDelimiter() == null
          && dlpReidentifyText.getHeaderColumns() != null) {
        throw new IllegalArgumentException(
            "Column delimiter should be set if headers are present.");
      }
      if (dlpReidentifyText.getHeaderColumns() == null
          && dlpReidentifyText.getColumnDelimiter() != null) {
        throw new IllegalArgumentException(
            "Column headers should be supplied when delimiter is present.");
      }
      return dlpReidentifyText;
    }
  }

  public static DLPReidentifyText.Builder newBuilder() {
    return new AutoValue_DLPReidentifyText.Builder();
  }

  /**
   * The transform converts the contents of input PCollection into {@link Table.Row}s and then calls
   * Cloud DLP service to perform the reidentification according to provided settings.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollection<KV<String, ReidentifyContentResponse>> expand(
      PCollection<KV<String, String>> input) {
    ParDo.SingleOutput<KV<String, Iterable<Table.Row>>, KV<String, ReidentifyContentResponse>>
        reidentifyParDo =
            ParDo.of(
                new ReidentifyText(
                    getProjectId(),
                    getInspectTemplateName(),
                    getReidentifyTemplateName(),
                    getInspectConfig(),
                    getReidentifyConfig(),
                    getHeaderColumns()));
    if (getHeaderColumns() != null) {
      reidentifyParDo = reidentifyParDo.withSideInputs(getHeaderColumns());
    }
    return input
        .apply(ParDo.of(new MapStringToDlpRow(getColumnDelimiter())))
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(getBatchSizeBytes())))
        .apply("DLPReidentify", reidentifyParDo);
  }

  /** Performs the calls to Cloud DLP service on GCP. */
  static class ReidentifyText
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, ReidentifyContentResponse>> {
    private final String projectId;
    private final String inspectTemplateName;
    private final String reidentifyTemplateName;
    private final InspectConfig inspectConfig;
    private final DeidentifyConfig reidentifyConfig;
    private final PCollectionView<List<String>> headerColumns;
    private transient ReidentifyContentRequest.Builder requestBuilder;
    private transient DlpServiceClient dlpServiceClient;

    @Setup
    public void setup() throws IOException {
      requestBuilder =
          ReidentifyContentRequest.newBuilder().setParent(ProjectName.of(projectId).toString());
      if (inspectTemplateName != null) {
        requestBuilder.setInspectTemplateName(inspectTemplateName);
      }
      if (inspectConfig != null) {
        requestBuilder.setInspectConfig(inspectConfig);
      }
      if (reidentifyConfig != null) {
        requestBuilder.setReidentifyConfig(reidentifyConfig);
      }
      if (reidentifyTemplateName != null) {
        requestBuilder.setReidentifyTemplateName(reidentifyTemplateName);
      }
      dlpServiceClient = DlpServiceClient.create();
    }

    @Teardown
    public void teardown() {
      dlpServiceClient.close();
    }

    /**
     * @param projectId ID of GCP project that should be used for deidentification.
     * @param inspectTemplateName Template name for inspection. Optional.
     * @param reidentifyTemplateName Template name for reidentification. Either this or
     *     reidentifyConfig is required.
     * @param inspectConfig Configuration object for inspection. Optional.
     * @param reidentifyConfig Reidentification config containing data transformations. Either this
     *     or reidentifyTemplateName is required.
     * @param headerColumns Header row of the table if applicable.
     */
    public ReidentifyText(
        String projectId,
        String inspectTemplateName,
        String reidentifyTemplateName,
        InspectConfig inspectConfig,
        DeidentifyConfig reidentifyConfig,
        PCollectionView<List<String>> headerColumns) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.reidentifyTemplateName = reidentifyTemplateName;
      this.inspectConfig = inspectConfig;
      this.reidentifyConfig = reidentifyConfig;
      this.headerColumns = headerColumns;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      List<FieldId> tableHeaders;
      if (headerColumns != null) {
        tableHeaders =
            context.sideInput(headerColumns).stream()
                .map(header -> FieldId.newBuilder().setName(header).build())
                .collect(Collectors.toList());
      } else {
        // handle unstructured input.
        tableHeaders = new ArrayList<>();
        tableHeaders.add(FieldId.newBuilder().setName("value").build());
      }
      Table table =
          Table.newBuilder()
              .addAllHeaders(tableHeaders)
              .addAllRows(context.element().getValue())
              .build();
      ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
      this.requestBuilder.setItem(contentItem);
      ReidentifyContentResponse response =
          dlpServiceClient.reidentifyContent(requestBuilder.build());
      context.output(KV.of(context.element().getKey(), response));
    }
  }
}
