/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.dataflow;

import com.google.common.base.CaseFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.beam.sdk.PipelineResult;

/** Utilities to make working with Dataflow easier. */
public class DataflowUtils {

  private DataflowUtils() {}

  public static boolean waitUntilState(
      PipelineResult pipeline, PipelineResult.State expectedState, Long timeoutMillis)
      throws InterruptedException {
    Instant start = Instant.now();
    while (true) {
      Thread.sleep(5_000);
      if (pipeline.getState().equals(expectedState)) {
        return true;
      }
      if (Duration.between(start, Instant.now()).compareTo(Duration.ofMillis(timeoutMillis)) > 0) {
        return false;
      }
    }
  }

  /**
   * Creates a job name.
   *
   * <p>If there are uppercase characters in {@code prefix}, then this will convert them into a dash
   * followed by the lowercase equivalent of that letter.
   *
   * <p>The job name will normally be unique, but this is not guaranteed if multiple jobs with the
   * same prefix are requested in a short period of time.
   *
   * @param prefix a prefix for the job
   * @return the prefix plus some way of identifying it separate from other jobs with the same
   *     prefix
   */
  public static String createJobName(String prefix) {
    String convertedPrefix =
        CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_HYPHEN).convert(prefix);
    String formattedTimestamp =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
            .withZone(ZoneId.of("UTC"))
            .format(Instant.now());
    return String.format("%s-%s", convertedPrefix, formattedTimestamp);
  }
}
