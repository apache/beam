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
package org.apache.beam.testinfra.pipelines.bigquery;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.DatasetReference;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Internal;

/** Parses Pipeline option value into a {@link DatasetReference}. */
@Internal
public class DatasetReferenceOptionValue implements Serializable {

  // For parsing the format used to parse a String into a dataset reference.
  // "{project_id}:{dataset_id}" or
  // "{project_id}.{dataset_id}"
  private static final Pattern DATASET_PATTERN =
      Pattern.compile("^(?<PROJECT>[^\\.:]+)[\\.:](?<DATASET>[^\\.:]+)$");

  private final String project;

  private final String dataset;

  DatasetReferenceOptionValue(String input) {
    Matcher m = DATASET_PATTERN.matcher(input);
    checkArgument(
        m.matches(),
        "input does not match BigQuery dataset pattern, "
            + "expected 'project_id.dataset_id' or 'project_id:dataset_id, got: %s",
        input);
    this.project = checkStateNotNull(m.group("PROJECT"), "PROJECT not found in %s", input);
    this.dataset = checkStateNotNull(m.group("DATASET"), "DATASET not found in %s", input);
  }

  /** Get the parsed String as a {@link DatasetReference}. */
  public DatasetReference getValue() {
    return new DatasetReference().setProjectId(this.project).setDatasetId(this.dataset);
  }
}
