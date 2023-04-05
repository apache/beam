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
/**
 * Defines {@link org.apache.beam.sdk.transforms.PTransform}s for transforming data in a pipeline.
 *
 * <p>A {@link org.apache.beam.sdk.transforms.PTransform} is an operation that takes an {@code
 * InputT} (some subtype of {@link org.apache.beam.sdk.values.PInput}) and produces an {@code
 * OutputT} (some subtype of {@link org.apache.beam.sdk.values.POutput}).
 *
 * <p>Common PTransforms include root PTransforms like {@link org.apache.beam.sdk.io.TextIO.Read}
 * and {@link org.apache.beam.sdk.transforms.Create}, processing and conversion operations like
 * {@link org.apache.beam.sdk.transforms.ParDo}, {@link org.apache.beam.sdk.transforms.GroupByKey},
 * {@link org.apache.beam.sdk.transforms.join.CoGroupByKey}, {@link
 * org.apache.beam.sdk.transforms.Combine}, and {@link org.apache.beam.sdk.transforms.Count}, and
 * outputting PTransforms like {@link org.apache.beam.sdk.io.TextIO.Write}.
 *
 * <p>New PTransforms can be created by composing existing PTransforms. Most PTransforms in this
 * package are composites, and users can also create composite PTransforms for their own
 * application-specific logic.
 */
@DefaultAnnotation(NonNull.class)
package org.apache.beam.sdk.transforms;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import org.checkerframework.checker.nullness.qual.NonNull;
