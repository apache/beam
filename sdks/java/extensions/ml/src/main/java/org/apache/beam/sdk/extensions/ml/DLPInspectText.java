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
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
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
 * inspecting text for identifying data according to provided settings. The transform supports both
 * CSV formatted input data and unstructured input.
 *
 * <p>If the csvHeader property is set and a sideinput with CSV headers is added to the PTransform,
 * csvDelimiter also should be set, else the results will be incorrect. If csvHeader is neither set
 * nor passed as sideinput, input is assumed to be unstructured.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>The transform consumes {@link KV} of {@link String}s (assumed to be filename as key and
 * contents as value) and outputs {@link KV} of {@link String} (eg. filename) and {@link
 * InspectContentResponse}, which will contain {@link Table} of results for the user to consume.
 *
 * <p>Either inspectTemplateName (String) or inspectConfig {@link InspectConfig} need to be set.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 */
@Experimental
@AutoValue
public abstract class DLPInspectText
    extends PTransform<
        PCollection<KV<String, String>>, PCollection<KV<String, InspectContentResponse>>> {

  public static final Integer DLP_PAYLOAD_LIMIT_BYTES = 524000;
  /** @return Template name for data inspection. */
  @Nullable
  public abstract String inspectTemplateName();
  /**
   * @return Configuration object for data inspection. If present, supersedes the template settings.
   */
  @Nullable
  public abstract InspectConfig inspectConfig();
  /** @return Size of input elements batch to be sent to Cloud DLP service in one request. */
  public abstract Integer batchSizeBytes();
  /** @return ID of Google Cloud project to be used when deidentifying data. */
  public abstract String projectId();
  /** @return Delimiter to be used when splitting values from input strings into columns. */
  @Nullable
  public abstract String csvColumnDelimiter();
  /** @return List of column names if the input KV value is a CSV formatted row. */
  @Nullable
  public abstract PCollectionView<List<String>> csvHeader();

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
     * @param batchSize Size of input elements batch to be sent to Cloud DLP service in one request.
     */
    public abstract Builder setBatchSizeBytes(Integer batchSize);
    /** @param projectId ID of Google Cloud project to be used when deidentifying data. */
    public abstract Builder setProjectId(String projectId);
    /**
     * @param csvDelimiter Delimiter to be used when splitting values from input strings into
     *     columns.
     */
    public abstract Builder setCsvColumnDelimiter(String csvDelimiter);
    /** @param csvHeader List of column names if the input KV value is a CSV formatted row. */
    public abstract Builder setCsvHeader(PCollectionView<List<String>> csvHeader);

    abstract DLPInspectText autoBuild();

    public DLPInspectText build() {
      DLPInspectText inspectText = autoBuild();
      if (inspectText.inspectTemplateName() == null && inspectText.inspectConfig() == null) {
        throw new IllegalArgumentException(
            "Either inspectTemplateName or inspectConfig must be supplied!");
      }
      if (inspectText.batchSizeBytes() > DLP_PAYLOAD_LIMIT_BYTES) {
        throw new IllegalArgumentException(
            String.format(
                "Batch size is too large! It should be smaller or equal than %d.",
                DLP_PAYLOAD_LIMIT_BYTES));
      }
      return inspectText;
    }
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPInspectText.Builder();
  }

  /**
   * The transform converts the contents of input PCollection into {@link Table.Row}s and then calls
   * Cloud DLP service to perform the data inspection according to provided settings.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollection<KV<String, InspectContentResponse>> expand(
      PCollection<KV<String, String>> input) {
    return input
        .apply(ParDo.of(new MapStringToDlpRow(csvColumnDelimiter())))
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(batchSizeBytes())))
        .apply(
            "DLPInspect",
            ParDo.of(
                new InspectData(projectId(), inspectTemplateName(), inspectConfig(), csvHeader())));
  }

  /** Performs calls to Cloud DLP service on GCP to inspect input data. */
  static class InspectData
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, InspectContentResponse>> {
    private final String projectId;
    private final String inspectTemplateName;
    private final InspectConfig inspectConfig;
    private transient InspectContentRequest.Builder requestBuilder;
    private final PCollectionView<List<String>> csvHeader;

    /**
     * @param projectId ID of GCP project that should be used for data inspection.
     * @param inspectTemplateName Template name for inspection.
     * @param inspectConfig Configuration object for inspection.
     * @param csvHeader Header row of CSV table if applicable.
     */
    public InspectData(
        String projectId,
        String inspectTemplateName,
        InspectConfig inspectConfig,
        PCollectionView<List<String>> csvHeader) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.inspectConfig = inspectConfig;
      this.csvHeader = csvHeader;
    }

    @Setup
    public void setup() {
      this.requestBuilder =
          InspectContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString());
      if (inspectTemplateName != null) {
        requestBuilder.setInspectTemplateName(this.inspectTemplateName);
      }
      if (inspectConfig != null) {
        requestBuilder.setInspectConfig(inspectConfig);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {
        List<FieldId> tableHeaders;
        if (csvHeader != null) {
          tableHeaders =
              c.sideInput(csvHeader).stream()
                  .map(header -> FieldId.newBuilder().setName(header).build())
                  .collect(Collectors.toList());
        } else {
          tableHeaders = new ArrayList<>();
          tableHeaders.add(FieldId.newBuilder().setName("value").build());
        }
        Table table =
            Table.newBuilder()
                .addAllHeaders(tableHeaders)
                .addAllRows(c.element().getValue())
                .build();
        ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
        this.requestBuilder.setItem(contentItem);
        InspectContentResponse response =
            dlpServiceClient.inspectContent(this.requestBuilder.build());
        c.output(KV.of(c.element().getKey(), response));
      }
    }
  }
}
