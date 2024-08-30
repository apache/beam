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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.gcp.util.GceMetadataUtil;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Metadata class for BigQueryIO. i.e. to use as BQ job labels. */
final class BigQueryIOMetadata {

  private final @Nullable String beamJobId;

  private final @Nullable String beamJobName;

  private final @Nullable String beamWorkerId;

  static final Supplier<BigQueryIOMetadata> INSTANCE =
      Suppliers.memoizeWithExpiration(() -> refreshInstance(), 5, TimeUnit.MINUTES);

  private BigQueryIOMetadata(
      @Nullable String beamJobId, @Nullable String beamJobName, @Nullable String beamWorkerId) {
    this.beamJobId = beamJobId;
    this.beamJobName = beamJobName;
    this.beamWorkerId = beamWorkerId;
  }

  private static final Pattern VALID_CLOUD_LABEL_PATTERN =
      Pattern.compile("^[a-z0-9\\_\\-]{1,63}$");

  /**
   * Creates a BigQueryIOMetadata. This will request metadata properly based on which runner is
   * being used.
   */
  public static BigQueryIOMetadata create() {
    return INSTANCE.get();
  }

  private static BigQueryIOMetadata refreshInstance() {
    String dataflowJobId = GceMetadataUtil.fetchDataflowJobId();
    // If a Dataflow job id is returned on GCE metadata. Then it means
    // this program is running on a Dataflow GCE VM.
    if (dataflowJobId.isEmpty() || !BigQueryIOMetadata.isValidCloudLabel(dataflowJobId)) {
      return new BigQueryIOMetadata(null, null, null);
    }

    return new BigQueryIOMetadata(
        dataflowJobId,
        GceMetadataUtil.fetchDataflowJobName(),
        GceMetadataUtil.fetchDataflowWorkerId());
  }

  public Map<String, String> addAdditionalJobLabels(Map<String, String> jobLabels) {
    if (this.beamJobId != null && !jobLabels.containsKey("beam_job_id")) {
      jobLabels.put("beam_job_id", this.beamJobId);
    }
    return jobLabels;
  }

  /*
   * Returns the beam job id. Can be null if it is not running on Dataflow.
   */
  public @Nullable String getBeamJobId() {
    return this.beamJobId;
  }

  /*
   * Returns the beam job name. Can be null if it is not running on Dataflow.
   */
  public @Nullable String getBeamJobName() {
    return this.beamJobName;
  }

  /*
   * Returns the beam worker id. Can be null if it is not running on Dataflow.
   */
  public @Nullable String getBeamWorkerId() {
    return this.beamWorkerId;
  }

  /**
   * Returns true if label_value is a valid cloud label string. This function can return false in
   * cases where the label value is valid. However, it will not return true in a case where the
   * label value is invalid. This is because a stricter set of allowed characters is used in this
   * validator, because foreign language characters are not accepted. Thus, this should not be used
   * as a generic validator for all cloud labels.
   *
   * <p>See Also: https://cloud.google.com/compute/docs/labeling-resources
   *
   * @return true if label_value is a valid cloud label string.
   */
  public static boolean isValidCloudLabel(String value) {
    Matcher m = VALID_CLOUD_LABEL_PATTERN.matcher(value);
    return m.find();
  }
}
