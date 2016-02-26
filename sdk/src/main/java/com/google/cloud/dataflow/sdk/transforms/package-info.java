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
 * Defines {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s for transforming
 * data in a pipeline.
 *
 * <p>A {@link com.google.cloud.dataflow.sdk.transforms.PTransform} is an operation that takes an
 * {@code InputT} (some subtype of {@link com.google.cloud.dataflow.sdk.values.PInput})
 * and produces an
 * {@code OutputT} (some subtype of {@link com.google.cloud.dataflow.sdk.values.POutput}).
 *
 * <p>Common PTransforms include root PTransforms like
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Read} and
 * {@link com.google.cloud.dataflow.sdk.transforms.Create}, processing and
 * conversion operations like {@link com.google.cloud.dataflow.sdk.transforms.ParDo},
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey},
 * {@link com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey},
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine}, and
 * {@link com.google.cloud.dataflow.sdk.transforms.Count}, and outputting
 * PTransforms like
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Write}.
 *
 * <p>New PTransforms can be created by composing existing PTransforms.
 * Most PTransforms in this package are composites, and users can also create composite PTransforms
 * for their own application-specific logic.
 *
 */
package com.google.cloud.dataflow.sdk.transforms;

