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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.service.AutoService;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;

/** Exposes {@link SpannerIO.WriteRows} as an external transform for cross-language usage. */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class SpannerTransformRegistrar implements ExternalTransformRegistrar {

  public static final String URN = "beam:external:java:spanner:write_rows:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
    return ImmutableMap.of(URN, WriteBuilder.class);
  }

  public abstract static class CrossLanguageConfiguration {
    String instanceId;
    String databaseId;
    @Nullable String projectId;

    public void setInstanceId(String instanceId) {
      this.instanceId = instanceId;
    }

    public void setDatabaseId(String databaseId) {
      this.databaseId = databaseId;
    }

    public void setProjectId(@Nullable String projectId) {
      this.projectId = projectId;
    }
  }

  @Experimental(Kind.PORTABILITY)
  public static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<Row>, PDone> {

    public static class Configuration extends CrossLanguageConfiguration {
      @Nullable private Long batchSizeBytes; // 1048576 = 1Mb
      @Nullable private Long maxNumMutations; // 5000
      @Nullable private Long maxNumRows; // 500
      @Nullable private FailureMode failureMode; // default: FAIL_FAST, jeszcze REPORT_FAILURES
      @Nullable private Integer groupingFactor; // 1000 default
      @Nullable private String host;
      @Nullable private Duration commitDeadline; // default 15s
      @Nullable private Duration maxCumulativeBackoff; // default 15min

      public void setBatchSizeBytes(@Nullable Long batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
      }

      public void setMaxNumMutations(@Nullable Long maxNumMutations) {
        this.maxNumMutations = maxNumMutations;
      }

      public void setMaxNumRows(@Nullable Long maxNumRows) {
        this.maxNumRows = maxNumRows;
      }

      public void setFailureMode(@Nullable String failureMode) {
        if (failureMode != null) {
          this.failureMode = FailureMode.valueOf(failureMode);
        }
      }

      public void setGroupingFactor(@Nullable Long groupingFactor) {
        if (groupingFactor != null) {
          this.groupingFactor = groupingFactor.intValue();
        }
      }

      public void setHost(@Nullable String host) {
        this.host = host;
      }

      public void setCommitDeadline(@Nullable Long commitDeadline) {
        if (commitDeadline != null) {
          this.commitDeadline = Duration.standardSeconds(commitDeadline);
        }
      }

      public void setMaxCumulativeBackoff(@Nullable Long maxCumulativeBackoff) {
        if (maxCumulativeBackoff != null) {
          this.maxCumulativeBackoff = Duration.standardSeconds(maxCumulativeBackoff);
        }
      }
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildExternal(Configuration configuration) {
      SpannerIO.Write writeTransform =
          SpannerIO.write()
              .withDatabaseId(configuration.databaseId)
              .withInstanceId(configuration.instanceId);
      if (configuration.projectId != null) {
        writeTransform = writeTransform.withProjectId(configuration.projectId);
      }
      if (configuration.batchSizeBytes != null) {
        writeTransform = writeTransform.withBatchSizeBytes(configuration.batchSizeBytes);
      }
      if (configuration.maxNumMutations != null) {
        writeTransform = writeTransform.withMaxNumMutations(configuration.maxNumMutations);
      }
      if (configuration.maxNumRows != null) {
        writeTransform = writeTransform.withMaxNumRows(configuration.maxNumRows);
      }
      if (configuration.failureMode != null) {
        writeTransform = writeTransform.withFailureMode(configuration.failureMode);
      }
      if (configuration.groupingFactor != null) {
        writeTransform = writeTransform.withGroupingFactor(configuration.groupingFactor);
      }
      if (configuration.host != null) {
        writeTransform = writeTransform.withHost(configuration.host);
      }
      if (configuration.commitDeadline != null) {
        writeTransform = writeTransform.withCommitDeadline(configuration.commitDeadline);
      }
      if (configuration.maxCumulativeBackoff != null) {
        writeTransform =
            writeTransform.withMaxCumulativeBackoff(configuration.maxCumulativeBackoff);
      }
      return SpannerIO.WriteRows.of(writeTransform);
    }
  }
}
