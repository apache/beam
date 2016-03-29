/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Defines transforms for reading and writing common storage formats, including
 * {@link com.google.cloud.dataflow.sdk.io.AvroIO},
 * {@link com.google.cloud.dataflow.sdk.io.BigQueryIO}, and
 * {@link com.google.cloud.dataflow.sdk.io.TextIO}.
 *
 * <p>The classes in this package provide {@code Read} transforms that create PCollections
 * from existing storage:
 * <pre>{@code
 * PCollection<TableRow> inputData = pipeline.apply(
 *     BigQueryIO.Read.named("Read")
 *                    .from("clouddataflow-readonly:samples.weather_stations");
 * }</pre>
 * and {@code Write} transforms that persist PCollections to external storage:
 * <pre> {@code
 * PCollection<Integer> numbers = ...;
 * numbers.apply(TextIO.Write.named("WriteNumbers")
 *                           .to("gs://my_bucket/path/to/numbers"));
 * } </pre>
 */
package com.google.cloud.dataflow.sdk.io;
