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
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.ReidentifyContentRequest;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} connecting to Cloud DLP and inspecting text for identifying data according
 * to provided settings.
 *
 * <p>Either inspectTemplateName (String) or inspectConfig {@link InspectConfig} need to be set, the
 * same goes for reidentifyTemplateName or reidentifyConfig.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 */
@Experimental
@AutoValue
public abstract class DLPReidentifyText
    extends PTransform<PCollection<KV<String, String>>, PCollection<KV<String, String>>> {

  public static final Logger LOG = LoggerFactory.getLogger(DLPInspectText.class);

  public static final Integer DLP_PAYLOAD_LIMIT = 52400;

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String reidentifyTemplateName();

  @Nullable
  public abstract InspectConfig inspectConfig();

  @Nullable
  public abstract DeidentifyConfig reidentifyConfig();

  public abstract Integer batchSize();

  public abstract String projectId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setInspectConfig(InspectConfig inspectConfig);

    public abstract Builder setReidentifyConfig(DeidentifyConfig deidentifyConfig);

    public abstract Builder setReidentifyTemplateName(String deidentifyTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract DLPReidentifyText build();
  }

  public static DLPReidentifyText.Builder newBuilder() {
    return new AutoValue_DLPReidentifyText.Builder();
  }

  @Override
  public PCollection<KV<String, String>> expand(PCollection<KV<String, String>> input) {
    return input
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(batchSize())))
        .apply(
            "DLPDeidentify",
            ParDo.of(
                new ReidentifyText(
                    projectId(),
                    inspectTemplateName(),
                    reidentifyTemplateName(),
                    inspectConfig(),
                    reidentifyConfig())));
  }

  public static class ReidentifyText extends DoFn<KV<String, String>, KV<String, String>> {
    private final String projectId;
    private final String inspectTemplateName;
    private final String reidentifyTemplateName;
    private final InspectConfig inspectConfig;
    private final DeidentifyConfig reidentifyConfig;
    private transient ReidentifyContentRequest.Builder requestBuilder;

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
      if (inspectConfig == null && inspectTemplateName == null) {
        throw new IllegalArgumentException(
            "Either inspectConfig or inspectTemplateName need to be set!");
      }
      if (reidentifyConfig != null) {
        requestBuilder.setReidentifyConfig(reidentifyConfig);
      }
      if (reidentifyTemplateName != null) {
        requestBuilder.setReidentifyTemplateName(reidentifyTemplateName);
      }
      if (reidentifyConfig == null && reidentifyTemplateName == null) {
        throw new IllegalArgumentException(
            "Either reidentifyConfig or reidentifyTemplateName need to be set!");
      }
    }

    public ReidentifyText(
        String projectId,
        String inspectTemplateName,
        String reidentifyTemplateName,
        InspectConfig inspectConfig,
        DeidentifyConfig reidentifyConfig) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.reidentifyTemplateName = reidentifyTemplateName;
      this.inspectConfig = inspectConfig;
      this.reidentifyConfig = reidentifyConfig;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      try (DlpServiceClient dlpServiceClient = DlpServiceClient.create()) {
        if (!c.element().getValue().isEmpty()) {
          ContentItem contentItem =
              ContentItem.newBuilder().setValue(c.element().getValue()).build();
          this.requestBuilder.setItem(contentItem);
          if (this.requestBuilder.build().getSerializedSize() > DLP_PAYLOAD_LIMIT) {
            String errorMessage =
                String.format(
                    "Payload Size %s Exceeded Batch Size %s",
                    this.requestBuilder.build().getSerializedSize(), DLP_PAYLOAD_LIMIT);
            LOG.error(errorMessage);
          } else {
            ReidentifyContentResponse response =
                dlpServiceClient.reidentifyContent(this.requestBuilder.build());
            response.getItem().getValue();
            c.output(KV.of(c.element().getKey(), response.getItem().getValue()));
          }
        }
      }
    }
  }
}
