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
package org.apache.beam.sdk.testutils.publishing;

import static java.lang.String.format;

import java.util.Collection;
import org.apache.beam.sdk.testutils.NamedTestResult;

/** Writes load test metrics results to console. */
public class ConsoleResultPublisher {

  public static void publish(Collection<NamedTestResult> results, String testId, String timestamp) {
    final String textTemplate = "%24s  %24s";

    System.out.println(
        format("Load test results for test (ID): %s and timestamp: %s:", testId, timestamp));
    System.out.println(format(textTemplate, "Metric:", "Value:"));

    results.forEach(
        (result) ->
            System.out.println(format(textTemplate, result.getMetric(), result.getValue())));
  }
}
