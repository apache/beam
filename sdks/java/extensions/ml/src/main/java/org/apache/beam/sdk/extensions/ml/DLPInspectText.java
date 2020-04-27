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
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
 * <p>Either inspectTemplateName (String) or inspectConfig {@link InspectConfig} need to be set.
 *
 * <p>Batch size defines how big are batches sent to DLP at once in bytes.
 */
@Experimental
@AutoValue
public abstract class DLPInspectText
    extends PTransform<PCollection<KV<String, String>>, PCollection<List<Finding>>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPInspectText.class);

  public static final Integer DLP_PAYLOAD_LIMIT = 52400;

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract InspectConfig inspectConfig();

  public abstract Integer batchSize();

  public abstract String projectId();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setInspectConfig(InspectConfig inspectConfig);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract DLPInspectText build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPInspectText.Builder();
  }

  @Override
  public PCollection<List<Finding>> expand(PCollection<KV<String, String>> input) {
    return input
        .apply("Batch Contents", ParDo.of(new BatchRequestForDLP(batchSize())))
        .apply(
            "DLPInspect",
            ParDo.of(new InspectData(projectId(), inspectTemplateName(), inspectConfig())));
  }

  public static class InspectData extends DoFn<KV<String, String>, List<Finding>> {
    private final String projectId;
    private final String inspectTemplateName;
    private final InspectConfig inspectConfig;
    private transient InspectContentRequest.Builder requestBuilder;
    private final Counter numberOfBytesInspected =
        Metrics.counter(InspectData.class, "NumberOfBytesInspected");

    public InspectData(String projectId, String inspectTemplateName, InspectConfig inspectConfig) {
      this.projectId = projectId;
      this.inspectTemplateName = inspectTemplateName;
      this.inspectConfig = inspectConfig;
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
      if (inspectTemplateName == null && inspectConfig == null) {
        throw new IllegalArgumentException("");
      }
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
            InspectContentResponse response =
                dlpServiceClient.inspectContent(this.requestBuilder.build());
            List<Finding> findingsList = response.getResult().getFindingsList();
            c.output(findingsList);
            numberOfBytesInspected.inc(contentItem.getSerializedSize());
          }
        }
      }
    }
  }
}
