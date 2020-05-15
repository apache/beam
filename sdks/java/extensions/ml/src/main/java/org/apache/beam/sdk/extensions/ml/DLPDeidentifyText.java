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
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InspectConfig;
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
 * A {@link PTransform} connecting to Cloud DLP and deidentifying text according to provided
 * settings. The transform supports both CSV formatted input data and unstructured input.
 *
 * <p>If the csvHeader property is set, csvDelimiter also should be, else the results will be
 * incorrect. If csvHeader is not set, input is assumed to be unstructured.
 *
 * <p>Either inspectTemplateName (String) or inspectConfig {@link InspectConfig} need to be set. The
 * situation is the same with deidentifyTemplateName and deidentifyConfig ({@link DeidentifyConfig}.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 *
 * <p>The transform outputs {@link KV} of {@link String} (eg. filename) and {@link
 * DeidentifyContentResponse}, which will contain {@link Table} of results for the user to consume.
 */
@Experimental
@AutoValue
public abstract class DLPDeidentifyText
    extends PTransform<
        PCollection<KV<String, String>>, PCollection<KV<String, DeidentifyContentResponse>>> {

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String deidentifyTemplateName();

  @Nullable
  public abstract InspectConfig inspectConfig();

  @Nullable
  public abstract DeidentifyConfig deidentifyConfig();

  @Nullable
  public abstract PCollectionView<List<String>> csvHeader();

  @Nullable
  public abstract String csvDelimiter();

  public abstract Integer batchSize();

  public abstract String projectId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setCsvHeader(PCollectionView<List<String>> csvHeader);

    public abstract Builder setCsvDelimiter(String delimiter);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDeidentifyTemplateName(String deidentifyTemplateName);

    public abstract Builder setInspectConfig(InspectConfig inspectConfig);

    public abstract Builder setDeidentifyConfig(DeidentifyConfig deidentifyConfig);

    public abstract DLPDeidentifyText build();
  }

  public static DLPDeidentifyText.Builder newBuilder() {
    return new AutoValue_DLPDeidentifyText.Builder();
  }

  /**
   * The transform batches the contents of input PCollection and then calls Cloud DLP service to
   * perform the deidentification.
   *
   * @param input input PCollection
   * @return PCollection after transformations
   */
  @Override
  public PCollection<KV<String, DeidentifyContentResponse>> expand(
      PCollection<KV<String, String>> input) {
    return input
        .apply(ParDo.of(new MapStringToDlpRow(csvDelimiter())))
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(batchSize())))
        .apply(
            "DLPDeidentify",
            ParDo.of(
                new DeidentifyText(
                    projectId(),
                    inspectTemplateName(),
                    deidentifyTemplateName(),
                    inspectConfig(),
                    deidentifyConfig(),
                    csvHeader())));
  }

  static class DeidentifyText
      extends DoFn<KV<String, Iterable<Table.Row>>, KV<String, DeidentifyContentResponse>> {
    private final String projectId;
    private final String inspectTemplateName;
    private final String deidentifyTemplateName;
    private final InspectConfig inspectConfig;
    private final DeidentifyConfig deidentifyConfig;
    private final PCollectionView<List<String>> csvHeaders;
    private transient DeidentifyContentRequest.Builder requestBuilder;

    @Setup
    public void setup() throws IOException {
      requestBuilder =
          DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(projectId).toString());
      if (inspectTemplateName != null) {
        requestBuilder.setInspectTemplateName(inspectTemplateName);
      }
      if (inspectConfig != null) {
        requestBuilder.setInspectConfig(inspectConfig);
      }
      if (inspectConfig == null && inspectTemplateName == null) {
        throw new IllegalArgumentException(
            "Either inspectConfig or inspectTemplateName need to be set!");
      }
      if (deidentifyConfig != null) {
        requestBuilder.setDeidentifyConfig(deidentifyConfig);
      }
      if (deidentifyTemplateName != null) {
        requestBuilder.setDeidentifyTemplateName(deidentifyTemplateName);
      }
      if (deidentifyConfig == null && deidentifyTemplateName == null) {
        throw new IllegalArgumentException(
            "Either deidentifyConfig or deidentifyTemplateName need to be set!");
      }
    }

    public DeidentifyText(
        String projectId,
        String inspectTemplateName,
        String deidentifyTemplateName,
        InspectConfig inspectConfig,
        DeidentifyConfig deidentifyConfig,
        PCollectionView<List<String>> csvHeaders) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.deidentifyTemplateName = deidentifyTemplateName;
      this.inspectConfig = inspectConfig;
      this.deidentifyConfig = deidentifyConfig;
      this.csvHeaders = csvHeaders;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {
        String fileName = c.element().getKey();
        List<FieldId> dlpTableHeaders;
        if (csvHeaders != null) {
          dlpTableHeaders =
              c.sideInput(csvHeaders).stream()
                  .map(header -> FieldId.newBuilder().setName(header).build())
                  .collect(Collectors.toList());
        } else {
          // handle unstructured input
          dlpTableHeaders = new ArrayList<>();
          dlpTableHeaders.add(FieldId.newBuilder().setName("value").build());
        }
        Table table =
            Table.newBuilder()
                .addAllHeaders(dlpTableHeaders)
                .addAllRows(c.element().getValue())
                .build();
        ContentItem contentItem = ContentItem.newBuilder().setTable(table).build();
        this.requestBuilder.setItem(contentItem);
        DeidentifyContentResponse response =
            dlpServiceClient.deidentifyContent(this.requestBuilder.build());
        c.output(KV.of(fileName, response));
      }
    }
  }
}
